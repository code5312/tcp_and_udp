#include <iostream>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <cstring>
#include <csignal>
#include <cerrno>
#include <vector>
#include <deque>
#include <map>
#include <set>
#include <string>
#include <algorithm>

#define PORT 8080
#define BACKLOG 5
#define BUFFER_SIZE 1024
#define MAX_EVENTS 64
#define MAX_OUTBOX_SIZE (64 * 1024) // 상대가 계속 안 읽어가면 outbox가 무한정 쌓이므로 상한을 둠
#define MAX_LINE_SIZE (64 * 1024)   // 개행 없이 한없이 긴 메시지가 들어오는 것도 상한을 둠

using namespace std;

// fd를 non-blocking 모드로 전환 (실패해도 치명적이진 않아서 bool만 반환)
bool setNonBlocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) {
        perror("fcntl(F_GETFL) failed");
        return false;
    }
    if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
        perror("fcntl(F_SETFL) failed");
        return false;
    }
    return true;
}

int main() {
    signal(SIGPIPE, SIG_IGN);

    int listen_fd;
    struct sockaddr_in server_addr;
    int opt = 1;

    listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) {
        perror("socket failed");
        return 1;
    }

    if (setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        perror("setsockopt failed");
        return 1;
    }

    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(PORT);

    if (bind(listen_fd, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        perror("bind failed");
        return 1;
    }

    if (listen(listen_fd, BACKLOG) < 0) {
        perror("listen failed");
        return 1;
    }

    if (!setNonBlocking(listen_fd)) {
        return 1; // non-blocking 설계 전체가 이 전제에 의존하므로 실패 시 그냥 종료
    }

    // select와 달리, 감시할 fd를 커널에 한 번만 등록해두면 됨 (매 루프 재등록 불필요)
    int epoll_fd = epoll_create1(0);
    if (epoll_fd < 0) {
        perror("epoll_create1 failed");
        return 1;
    }

    struct epoll_event ev;
    ev.events = EPOLLIN;
    ev.data.fd = listen_fd;
    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, listen_fd, &ev) < 0) {
        perror("epoll_ctl (listen_fd) failed");
        return 1;
    }

    cout << "서버가 포트 " << PORT << "에서 연결을 대기 중입니다... (epoll)\n";

    deque<int> waiting_queue; // 매칭 대기열
    map<int, int> partner;    // fd -> 매칭된 상대방 fd
    map<int, string> outbox;  // fd별로 커널 송신 버퍼가 꽉 차서 아직 못 보낸 데이터
    map<int, string> inbox;   // fd별로 아직 개행('\n')으로 끝나지 않은 수신 조각 (메시지 프레이밍용)
    vector<int> to_disconnect; // 이번 배치에서 끊기로 판단된 fd (즉시 close하지 않고 모아둠)
    char buffer[BUFFER_SIZE];

    // fd가 이번 배치에서 이미 끊기기로 결정되어 있는지 확인 (재매칭 가드에서 사용)
    auto isLeaving = [&](int fd) {
        return find(to_disconnect.begin(), to_disconnect.end(), fd) != to_disconnect.end();
    };

    // fd의 outbox를 non-blocking write로 최대한 비움.
    // 다 비웠거나 커널 송신 버퍼가 꽉 차서 잠시 못 보내는 상태(EAGAIN)면 true,
    // 그 외 에러(상대가 확실히 죽었다고 판단)면 false.
    auto flushOutbox = [&](int fd) -> bool {
        auto it = outbox.find(fd);
        if (it == outbox.end() || it->second.empty()) return true;
        string& buf = it->second;
        while (!buf.empty()) {
            ssize_t n = write(fd, buf.data(), buf.size());
            if (n > 0) {
                buf.erase(0, n);
                continue;
            }
            if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                return true; // 커널 송신 버퍼가 꽉 참 -> 다음 EPOLLOUT을 기다림
            }
            perror("write failed");
            return false;
        }
        outbox.erase(it);
        return true;
    };

    // outbox에 쌓아두고 즉시 flush를 시도. 당장 다 못 보내면 EPOLLOUT도 같이
    // 감시하도록 등록을 바꿔서, 다음에 소켓이 다시 쓰기 가능해질 때 이어서 보낸다.
    auto sendMsg = [&](int fd, const string& msg) {
        // outbox가 이미 비어있지 않았다면(= 이전 호출에서 이미 EPOLLOUT을 등록해뒀다면)
        // 아래에서 epoll_ctl(MOD)를 또 부를 필요가 없음 -> append 전에 미리 기억해둠
        auto existing = outbox.find(fd);
        bool wasPending = existing != outbox.end() && !existing->second.empty();

        outbox[fd] += msg;
        if (!flushOutbox(fd)) {
            to_disconnect.push_back(fd);
            return;
        }
        auto it = outbox.find(fd);
        if (it != outbox.end() && !it->second.empty()) {
            if (it->second.size() > MAX_OUTBOX_SIZE) {
                // 상대가 한참 안 읽어가서(느리거나 멈춘 클라이언트) 보낼 데이터가 계속 쌓이기만 함
                // -> 무한정 버티지 않고 그냥 끊는다 (backpressure를 안 걸면 서버 메모리가 계속 늘어남)
                cerr << "fd=" << fd << " outbox가 " << MAX_OUTBOX_SIZE << "바이트를 넘어 연결을 종료합니다.\n";
                to_disconnect.push_back(fd);
                return;
            }
            if (wasPending) return; // 이미 EPOLLOUT을 감시 중이므로 epoll_ctl을 다시 부를 필요 없음
            struct epoll_event mod_ev;
            mod_ev.events = EPOLLIN | EPOLLOUT;
            mod_ev.data.fd = fd;
            epoll_ctl(epoll_fd, EPOLL_CTL_MOD, fd, &mod_ev);
        }
    };

    auto tryMatch = [&]() {
        // 대기열에 남아있지만 이번 배치에서 이미 끊기기로 정해진 fd는 매칭 후보에서 제외.
        // 그냥 두면 곧 사라질 fd가 멀쩡한 제3자와 짝지어졌다가 그 fd의 disconnect() 처리
        // 순서가 왔을 때 곧바로 풀려버리는 유령 매칭이 생김(함정 2/3과 같은 클래스의 문제인데,
        // 그 가드들은 partner였던 상대만 확인했지 대기열 자체는 확인하지 않아서 놓쳤던 경로).
        waiting_queue.erase(
            remove_if(waiting_queue.begin(), waiting_queue.end(), isLeaving),
            waiting_queue.end());

        while (waiting_queue.size() >= 2) {
            int a = waiting_queue.front(); waiting_queue.pop_front();
            int b = waiting_queue.front(); waiting_queue.pop_front();
            partner[a] = b;
            partner[b] = a;
            cout << "매칭 성사: fd=" << a << " <-> fd=" << b << "\n";
            sendMsg(a, "매칭 성사되었습니다! 상대방과 대화를 시작하세요.\n");
            sendMsg(b, "매칭 성사되었습니다! 상대방과 대화를 시작하세요.\n");
        }
    };

    // 클라이언트 연결이 끊겼을 때 대기열/매칭 상태를 정리하고 epoll 감시에서 제거
    auto disconnect = [&](int fd) {
        auto qit = find(waiting_queue.begin(), waiting_queue.end(), fd);
        if (qit != waiting_queue.end()) {
            waiting_queue.erase(qit);
        } else {
            auto it = partner.find(fd);
            if (it != partner.end()) {
                int other_fd = it->second;
                partner.erase(fd);
                partner.erase(other_fd);
                // 상대방도 같은 배치에서 같이 끊어질 예정이면(둘이 거의 동시에 연결을 끊은 경우),
                // 곧 사라질 상대를 대기열에 되돌렸다가 엉뚱한 제3자와 순간적으로 재매칭시키고
                // 곧바로 다시 풀어버리는 유령 매칭이 생기므로 이 경우엔 재매칭을 건너뜀.
                // sendMsg() 안의 flushOutbox()가 실패해서 "알림을 보내다가" 상대가 새로
                // to_disconnect에 추가될 수도 있으므로, 보내기 전/후 두 번 다 확인해야 함.
                bool alreadyLeaving = isLeaving(other_fd);

                // 개행 없이 끝난(=아직 relay되지 못한) 마지막 메시지가 inbox에 남아있을 수
                // 있음 - 연결 종료 자체를 그 메시지의 끝으로 취급해서 사라지기 전에 상대에게
                // 마저 전달한다. (MAX_LINE_SIZE 초과로 거부된 경우는 그 자리에서 이미
                // inbox를 비워뒀으므로 여기서 다시 전달되지 않음.)
                if (!alreadyLeaving) {
                    auto inboxIt = inbox.find(fd);
                    if (inboxIt != inbox.end() && !inboxIt->second.empty()) {
                        // 원래 개행이 없어서 여기 남아있던 것이므로, 그대로 보내면 바로 뒤에
                        // 이어붙는 "상대방이 나갔습니다" 메시지와 한 줄로 붙어버림 -> 개행을
                        // 붙여서 별도의 한 줄로 도착하게 함.
                        sendMsg(other_fd, inboxIt->second + "\n");
                        alreadyLeaving = isLeaving(other_fd);
                    }
                }
                if (!alreadyLeaving) {
                    sendMsg(other_fd, "상대방이 나갔습니다. 다시 매칭 대기 중입니다...\n");
                    alreadyLeaving = isLeaving(other_fd);
                }
                if (!alreadyLeaving) {
                    waiting_queue.push_back(other_fd);
                    tryMatch();
                }
            }
        }
        // 아직 outbox에 못 보낸 데이터가 남아있을 수 있음(예: 상대에게 relay한 메시지가
        // 커널 송신 버퍼가 꽉 차서 EAGAIN으로 대기 중이던 상황에, 이 fd의 읽기 쪽에서
        // 독립적으로 EOF/에러가 발생해 disconnect가 호출된 경우) -> 그냥 버리기 전에
        // 마지막으로 한 번 더 flush를 시도해본다(성공/실패 여부와 무관하게 결과는 무시하고
        // 어차피 이제 정리하고 close할 것이므로).
        flushOutbox(fd);
        outbox.erase(fd);
        inbox.erase(fd);
        // close()만 해도 커널이 감시 목록에서 빼주지만, 명시적으로 지워서 의도를 분명히 함
        epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, NULL);
        close(fd);
    };

    struct epoll_event events[MAX_EVENTS];

    while (true) {
        // select와 달리 fd_set을 매번 재구성할 필요 없이, 등록해둔 것 중 "준비된 것만" 돌려받음
        int n_ready = epoll_wait(epoll_fd, events, MAX_EVENTS, -1);
        if (n_ready < 0) {
            perror("epoll_wait failed");
            break;
        }

        for (int i = 0; i < n_ready; ++i) {
            int fd = events[i].data.fd;

            if (fd == listen_fd) {
                // 새 클라이언트 연결
                int client_fd = accept(listen_fd, NULL, NULL);
                if (client_fd < 0) {
                    if (errno != EAGAIN && errno != EWOULDBLOCK) perror("accept failed");
                    continue;
                }

                if (!setNonBlocking(client_fd)) {
                    // non-blocking 전환에 실패한 fd는 blocking 상태로 섞여 들어가면
                    // write() 한 번이 이벤트 루프 전체를 멈출 수 있어 아예 받지 않음
                    close(client_fd);
                    continue;
                }
                ev.events = EPOLLIN;
                ev.data.fd = client_fd;
                if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &ev) < 0) {
                    perror("epoll_ctl (client_fd) failed");
                    close(client_fd);
                    continue;
                }

                cout << "클라이언트가 연결되었습니다! (fd=" << client_fd << ")\n";

                waiting_queue.push_back(client_fd);
                tryMatch();
                if (!waiting_queue.empty() && waiting_queue.back() == client_fd) {
                    sendMsg(client_fd, "매칭 대기 중입니다...\n");
                }
                continue;
            }

            // 밀린 데이터를 보낼 수 있게 됐다는 이벤트
            if (events[i].events & EPOLLOUT) {
                if (!flushOutbox(fd)) {
                    to_disconnect.push_back(fd);
                    continue; // 곧 끊길 fd이므로 아래 읽기 처리는 건너뜀
                }
                auto it = outbox.find(fd);
                if (it == outbox.end() || it->second.empty()) {
                    // 다 보냈으면 굳이 매번 깨어날 필요 없음(소켓은 늘 writable 상태이므로
                    // EPOLLOUT을 계속 감시하면 busy-loop처럼 계속 이벤트가 옴) -> EPOLLIN만 남김
                    struct epoll_event mod_ev;
                    mod_ev.events = EPOLLIN;
                    mod_ev.data.fd = fd;
                    epoll_ctl(epoll_fd, EPOLL_CTL_MOD, fd, &mod_ev);
                }
            }

            // 기존 클라이언트: 데이터 도착 또는 연결 종료.
            // EPOLLHUP/EPOLLERR만 뜨고 EPOLLIN은 안 뜬 채로 이 조건에서 빠지면, 레벨 트리거인
            // epoll이 다음 루프에도 같은 이벤트를 계속 돌려줘서 아무 처리도 없이 busy-loop만
            // 돌게 됨 -> read()를 시도해서 0(EOF)/에러를 반환하게 만들어 정상적으로 끊어지게 함.
            if (events[i].events & (EPOLLIN | EPOLLHUP | EPOLLERR)) {
                // 더 이상 buffer를 C 문자열처럼 다루지 않으므로 null 종료 없이 꽉 채워 읽어도 됨
                ssize_t n = read(fd, buffer, sizeof(buffer));
                if (n > 0) {
                    // TCP는 스트림이라 이 한 번의 read()가 클라이언트가 보낸 "한 메시지"와
                    // 같다는 보장이 없음(여러 메시지가 붙어 오거나, 한 메시지가 잘려 올 수 있음).
                    // fd별 inbox에 누적한 뒤, 개행('\n')으로 끝나는 완전한 줄 단위로만 relay한다.
                    string& buf = inbox[fd];
                    buf.append(buffer, n);

                    size_t pos;
                    while ((pos = buf.find('\n')) != string::npos) {
                        string line = buf.substr(0, pos + 1); // 개행까지 포함해서 한 메시지로 취급
                        buf.erase(0, pos + 1);

                        cout << "[fd=" << fd << "] 수신된 메시지: " << line;

                        auto it = partner.find(fd);
                        if (it != partner.end()) {
                            sendMsg(it->second, line);
                        } else {
                            sendMsg(fd, "아직 매칭 대기 중입니다. 잠시만 기다려주세요.\n");
                        }
                    }

                    if (buf.size() > MAX_LINE_SIZE) {
                        // 개행 없이 한없이 긴 데이터를 보내는 클라이언트 -> 무한정 버티지 않고 끊음.
                        // disconnect()가 나중에 "끝맺음 없는 마지막 메시지"를 상대에게 마저 전달해주는데,
                        // 이 경우는 애초에 거부하려는 데이터이므로 미리 비워서 그 흐름을 타지 않게 함.
                        cerr << "fd=" << fd << " 한 줄이 " << MAX_LINE_SIZE << "바이트를 넘어 연결을 종료합니다.\n";
                        buf.clear();
                        to_disconnect.push_back(fd);
                    }
                } else if (n == 0) {
                    cout << "클라이언트(fd=" << fd << ")가 연결을 종료했습니다.\n";
                    to_disconnect.push_back(fd);
                } else if (errno != EAGAIN && errno != EWOULDBLOCK) {
                    perror("read failed");
                    to_disconnect.push_back(fd);
                }
            }
        }

        // 이번 배치에서 끊기로 한 fd들을 한 번에 정리.
        // 이벤트 처리 도중 바로 close()하면, 같은 epoll_wait 배치 안의 다른 이벤트가
        // (OS가 그 fd 번호를 재사용했을 경우) 엉뚱한 소켓을 가리키게 될 수 있어 모아뒀다가 처리.
        set<int> disconnected;
        for (size_t k = 0; k < to_disconnect.size(); ++k) {
            int fd = to_disconnect[k];
            if (disconnected.count(fd)) continue; // 같은 배치에서 중복 등록된 경우
            disconnected.insert(fd);
            disconnect(fd);
        }
        to_disconnect.clear();
    }

    close(listen_fd);
    close(epoll_fd);

    cout << "서버를 종료합니다.\n";

    return 0;
}

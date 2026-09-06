#include <iostream>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cstring>
#include <csignal>
#include <deque>
#include <map>
#include <string>
#include <algorithm>

#define PORT 8080
#define BACKLOG 5
#define BUFFER_SIZE 1024
#define MAX_EVENTS 64

using namespace std;

void sendMsg(int fd, const string& msg) {
    if (write(fd, msg.c_str(), msg.size()) < 0) {
        perror("write failed");
    }
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
    char buffer[BUFFER_SIZE];

    auto tryMatch = [&]() {
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
                sendMsg(other_fd, "상대방이 나갔습니다. 다시 매칭 대기 중입니다...\n");
                waiting_queue.push_back(other_fd);
                tryMatch();
            }
        }
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
                    perror("accept failed");
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

            // 기존 클라이언트: 데이터 도착 또는 연결 종료
            ssize_t n = read(fd, buffer, sizeof(buffer) - 1);
            if (n <= 0) {
                if (n == 0) {
                    cout << "클라이언트(fd=" << fd << ")가 연결을 종료했습니다.\n";
                } else {
                    perror("read failed");
                }
                disconnect(fd);
                continue;
            }

            buffer[n] = '\0';
            cout << "[fd=" << fd << "] 수신된 메시지: " << buffer;

            auto it = partner.find(fd);
            if (it != partner.end()) {
                sendMsg(it->second, string(buffer, n));
            } else {
                sendMsg(fd, "아직 매칭 대기 중입니다. 잠시만 기다려주세요.\n");
            }
        }
    }

    close(listen_fd);
    close(epoll_fd);

    cout << "서버를 종료합니다.\n";

    return 0;
}

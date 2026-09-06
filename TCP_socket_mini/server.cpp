#include <iostream>
#include <sys/socket.h>
#include <sys/select.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cstring>
#include <csignal>
#include <vector>
#include <deque>
#include <map>
#include <string>
#include <algorithm>

#define PORT 8080
#define BACKLOG 5
#define BUFFER_SIZE 1024

using namespace std;

// 클라이언트에게 안내 메시지를 보낼 때 쓰는 헬퍼 (실패해도 로그만 남기고 계속 진행)
void sendMsg(int fd, const string& msg) {
    if (write(fd, msg.c_str(), msg.size()) < 0) {
        perror("write failed");
    }
}

int main(){
    // 이미 끊긴 소켓에 write()하면 SIGPIPE가 발생해 기본 동작으로 프로세스 전체가 죽을 수 있음.
    // 무시해두면 write()가 그냥 -1(EPIPE)을 반환하고, 우리는 그걸 이미 perror로 처리하고 있음.
    signal(SIGPIPE, SIG_IGN);

    int listen_fd, client_fd; // 리스닝, 클라이언트 통신 소켓
    struct sockaddr_in server_addr;
    int opt = 1;

    // 1. 소켓 생성 (socket)
    listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) {
        perror("socket failed");
        return 1;
    }

    // 서버 재시작 시 "Address already in use" 에러 방지용
    if (setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        perror("setsockopt failed");
        return 1;
    }

    // 2. sockaddr_in 구조체 초기화 및 설정 (바이트 오더 변환)
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;  // 모든 IP에서의 접속 허용
    server_addr.sin_port = htons(PORT);        // 호스트 -> 네트워크 바이트 오더 변환

    // 3. 소켓 바인딩 (bind)
    if (bind(listen_fd, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        perror("bind failed");
        return 1;
    }

    // 4. 연결 대기 상태로 전환 (listen)
    if (listen(listen_fd, BACKLOG) < 0) {
        perror("listen failed");
        return 1;
    }

    cout << "서버가 포트 " << PORT << "에서 연결을 대기 중입니다...\n";

    // 5. select()로 listen_fd + 모든 client_fd를 동시에 감시
    // listen_fd가 읽기 가능 -> 새 연결 도착, client_fd가 읽기 가능 -> 데이터 도착(혹은 종료)
    vector<int> clients;
    fd_set read_fds;
    char buffer[BUFFER_SIZE];

    deque<int> waiting_queue; // 매칭 대기열 (변수 하나로 관리하면, 매칭된 쌍이 있는 도중
                              // 새 대기자가 생겼다가 기존 쌍이 깨질 때 그 대기자가
                              // 덮어써져 사라지는 문제가 있어서 큐로 관리함)
    map<int, int> partner;    // fd -> 매칭된 상대방 fd (매칭된 클라이언트만 존재)

    // 대기열에 2명 이상 쌓이면 앞에서부터 순서대로 짝지어줌
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

    while (true) {
        // select()는 감시 후 fd_set을 "준비된 fd만 남기고" 덮어써버리므로
        // 매 루프마다 다시 채워야 함
        FD_ZERO(&read_fds);
        FD_SET(listen_fd, &read_fds);
        int max_fd = listen_fd;

        for (int fd : clients) {
            FD_SET(fd, &read_fds);
            if (fd > max_fd) max_fd = fd;
        }

        // timeout=NULL -> 아무 fd도 준비 안 되면 무한 대기
        int ready = select(max_fd + 1, &read_fds, NULL, NULL, NULL);
        if (ready < 0) {
            perror("select failed");
            break;
        }

        // 새 클라이언트 연결
        if (FD_ISSET(listen_fd, &read_fds)) {
            client_fd = accept(listen_fd, NULL, NULL);
            if (client_fd < 0) {
                perror("accept failed");
            } else {
                clients.push_back(client_fd);
                cout << "클라이언트가 연결되었습니다! (fd=" << client_fd
                     << ", 현재 " << clients.size() << "명)\n";

                waiting_queue.push_back(client_fd);
                tryMatch();
                // tryMatch 후에도 여전히 대기열 맨 뒤에 남아있다면 짝을 못 찾은 것
                if (!waiting_queue.empty() && waiting_queue.back() == client_fd) {
                    sendMsg(client_fd, "매칭 대기 중입니다...\n");
                }
            }
        }

        // 기존 클라이언트들 처리 (연결 종료 시 벡터에서 제거해야 하므로 인덱스 수동 관리)
        for (size_t i = 0; i < clients.size(); ) {
            int fd = clients[i];
            if (!FD_ISSET(fd, &read_fds)) {
                ++i;
                continue;
            }

            ssize_t n = read(fd, buffer, sizeof(buffer) - 1);
            if (n <= 0) {
                if (n == 0) {
                    cout << "클라이언트(fd=" << fd << ")가 연결을 종료했습니다.\n";
                } else {
                    perror("read failed");
                }

                auto qit = find(waiting_queue.begin(), waiting_queue.end(), fd);
                if (qit != waiting_queue.end()) {
                    // 대기 중이던 클라이언트가 그냥 나간 경우
                    waiting_queue.erase(qit);
                } else {
                    auto it = partner.find(fd);
                    if (it != partner.end()) {
                        int other_fd = it->second;
                        partner.erase(fd);
                        partner.erase(other_fd);
                        // 상대방을 다시 대기열로 돌려보내고, 이미 기다리던 다른 사람이 있으면 즉시 재매칭
                        sendMsg(other_fd, "상대방이 나갔습니다. 다시 매칭 대기 중입니다...\n");
                        waiting_queue.push_back(other_fd);
                        tryMatch();
                    }
                }

                close(fd);
                clients.erase(clients.begin() + i); // 제거 후 같은 i가 다음 원소를 가리킴
                continue;
            }

            buffer[n] = '\0';
            cout << "[fd=" << fd << "] 수신된 메시지: " << buffer;

            auto it = partner.find(fd);
            if (it != partner.end()) {
                // 매칭된 상대방에게 그대로 전달 (relay) — 자기 자신에게는 에코하지 않음
                sendMsg(it->second, string(buffer, n));
            } else {
                sendMsg(fd, "아직 매칭 대기 중입니다. 잠시만 기다려주세요.\n");
            }
            ++i;
        }
    }

    // 6. 소켓 닫기 (close)
    for (int fd : clients) {
        close(fd);
    }
    close(listen_fd);

    cout << "서버를 종료합니다.\n";

    return 0;

}
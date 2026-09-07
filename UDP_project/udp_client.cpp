#include <iostream>
#include <sys/socket.h>
#include <sys/select.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cstring>

#define PORT 9090
#define BUFFER_SIZE 1024
#define HEARTBEAT_INTERVAL_SEC 3 // 서버의 IDLE_TIMEOUT_SEC(10)보다 충분히 짧아야 함

using namespace std;

int main(int argc, char* argv[]) {
    // UDP에는 SIGPIPE가 없음: 상대가 없는 주소로 write()해도 커널이 그 자리에서 프로세스를
    // 죽이는 시그널을 보내지 않음(TCP client.cpp에서 SIGPIPE를 막아야 했던 것과 대조적).
    // connect()된 UDP 소켓이라면 상대가 아예 응답 없는 포트일 때 다음 recv()에서
    // ECONNREFUSED가 뒤늦게(비동기로) 나타날 수는 있지만, 그마저도 시그널이 아니라 에러 코드임.

    const char* server_ip = (argc > 1) ? argv[1] : "127.0.0.1";

    int sock_fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock_fd < 0) {
        perror("socket failed");
        return 1;
    }

    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(PORT);
    if (inet_pton(AF_INET, server_ip, &server_addr.sin_addr) <= 0) {
        perror("inet_pton failed");
        return 1;
    }

    // UDP 소켓에도 connect()를 부를 수 있음. TCP처럼 3-way handshake가 실제로 일어나는 건
    // 아니고, 그냥 이 소켓의 "기본 목적지"를 커널에 고정해두는 것뿐 - 그 덕에 매번
    // sendto()/recvfrom()에 주소를 넘길 필요 없이 send()/recv()를 그대로 쓸 수 있고,
    // 이 주소가 아닌 곳에서 온 datagram은 커널이 알아서 걸러줌. 그래서 이 클라이언트는
    // TCP client.cpp와 거의 같은 구조로 짤 수 있음.
    if (connect(sock_fd, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        perror("connect failed");
        return 1;
    }

    cout << "서버(" << server_ip << ":" << PORT << ")로 접속을 알립니다 (UDP). "
         << "메시지를 입력하세요 (종료: Ctrl+D)\n";

    // UDP는 클라이언트가 먼저 뭔가 보내기 전까지 서버가 이 클라이언트의 존재 자체를 모름
    // (TCP의 connect()에 해당하는 "접속" 이벤트가 없음). 그래서 빈 datagram을 하나 보내서
    // "나 여기 있어요"라고 직접 알려줘야 함 - 서버는 이걸 매칭 대기열에 넣는 신호로만 쓰고
    // 내용으로는 취급하지 않는다(udp_server.cpp의 n>0 체크 참고).
    if (send(sock_fd, "", 0, 0) < 0) {
        perror("send(announce) failed");
    }

    char buffer[BUFFER_SIZE];
    fd_set read_fds;

    while (true) {
        FD_ZERO(&read_fds);
        FD_SET(STDIN_FILENO, &read_fds);
        FD_SET(sock_fd, &read_fds);
        int max_fd = (STDIN_FILENO > sock_fd) ? STDIN_FILENO : sock_fd;

        // TCP는 연결 자체가 살아있으면 아무 말도 안 해도 서버가 계속 "연결된 상태"로
        // 알아서 유지해주지만, UDP는 서버가 "마지막으로 datagram을 받은 시각"만으로 유휴를
        // 판단하므로(udp_server.cpp의 sweepIdle) 사용자가 그냥 상대 턴을 기다리며 아무것도
        // 입력하지 않으면 실제로는 멀쩡히 연결돼 있어도 타임아웃으로 쫓겨날 수 있음.
        // 그래서 일정 주기로 빈 datagram을 하트비트로 보내 "나 아직 살아있어요"를 알려야 함.
        struct timeval tv;
        tv.tv_sec = HEARTBEAT_INTERVAL_SEC;
        tv.tv_usec = 0;

        int ready = select(max_fd + 1, &read_fds, NULL, NULL, &tv);
        if (ready < 0) {
            perror("select failed");
            break;
        }

        if (ready == 0) {
            // 타임아웃 = 그동안 아무 입출력도 없었다는 뜻 -> 하트비트 전송
            if (send(sock_fd, "", 0, 0) < 0) {
                perror("send(heartbeat) failed");
                break;
            }
            continue;
        }

        if (FD_ISSET(sock_fd, &read_fds)) {
            ssize_t n = recv(sock_fd, buffer, sizeof(buffer), 0);
            if (n < 0) {
                perror("recv failed");
                break;
            }
            // n==0은 TCP처럼 "연결 종료"가 아니라 그냥 빈 datagram 수신을 뜻함
            // (서버가 클라이언트에게 빈 datagram을 보내는 경우는 없어서 실전에선 안 옴).
            if (n > 0) {
                cout.write(buffer, n);
                cout.flush();
            }
        }

        if (FD_ISSET(STDIN_FILENO, &read_fds)) {
            ssize_t n = read(STDIN_FILENO, buffer, sizeof(buffer));
            if (n <= 0) {
                cout << "입력이 종료되어 종료합니다.\n";
                break;
            }
            if (send(sock_fd, buffer, n, 0) < 0) {
                perror("send failed");
                break;
            }
        }
    }

    close(sock_fd);
    return 0;
}

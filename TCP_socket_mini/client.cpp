#include <iostream>
#include <sys/socket.h>
#include <sys/select.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cstring>
#include <csignal>

#define PORT 8080
#define BUFFER_SIZE 1024

using namespace std;

int main(int argc, char* argv[]) {
    signal(SIGPIPE, SIG_IGN); // 서버가 끊긴 뒤 write()해도 죽지 않고 -1(EPIPE)로 처리되게 함

    const char* server_ip = (argc > 1) ? argv[1] : "127.0.0.1";

    // 1. 소켓 생성
    int sock_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (sock_fd < 0) {
        perror("socket failed");
        return 1;
    }

    // 2. 서버 주소 설정
    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(PORT);
    if (inet_pton(AF_INET, server_ip, &server_addr.sin_addr) <= 0) {
        perror("inet_pton failed");
        return 1;
    }

    // 3. 서버에 연결 (connect) - accept()와 반대로 클라이언트가 능동적으로 시도
    if (connect(sock_fd, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        perror("connect failed");
        return 1;
    }

    cout << "서버(" << server_ip << ":" << PORT << ")에 연결되었습니다. 메시지를 입력하세요 (종료: Ctrl+D)\n";

    // 4. select()로 표준입력(키보드)과 소켓을 동시에 감시
    // - 소켓이 준비됨: 서버(혹은 relay된 상대방)로부터 메시지 도착
    // - 표준입력이 준비됨: 사용자가 뭔가 입력함
    char buffer[BUFFER_SIZE];
    fd_set read_fds;

    while (true) {
        FD_ZERO(&read_fds);
        FD_SET(STDIN_FILENO, &read_fds);
        FD_SET(sock_fd, &read_fds);
        int max_fd = (STDIN_FILENO > sock_fd) ? STDIN_FILENO : sock_fd;

        int ready = select(max_fd + 1, &read_fds, NULL, NULL, NULL);
        if (ready < 0) {
            perror("select failed");
            break;
        }

        if (FD_ISSET(sock_fd, &read_fds)) {
            ssize_t n = read(sock_fd, buffer, sizeof(buffer) - 1);
            if (n <= 0) {
                if (n == 0) {
                    cout << "서버와의 연결이 종료되었습니다.\n";
                } else {
                    perror("read failed");
                }
                break;
            }
            buffer[n] = '\0';
            cout << buffer;
            cout.flush();
        }

        if (FD_ISSET(STDIN_FILENO, &read_fds)) {
            ssize_t n = read(STDIN_FILENO, buffer, sizeof(buffer) - 1);
            if (n <= 0) {
                cout << "입력이 종료되어 연결을 닫습니다.\n";
                break;
            }
            buffer[n] = '\0';
            if (write(sock_fd, buffer, n) < 0) {
                perror("write failed");
                break;
            }
        }
    }

    // 5. 소켓 닫기
    close(sock_fd);

    return 0;
}

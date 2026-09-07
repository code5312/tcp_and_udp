#include <iostream>
#include <sys/socket.h>
#include <sys/select.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cstring>
#include <cerrno>
#include <ctime>
#include <deque>
#include <map>
#include <string>
#include <vector>
#include <algorithm>

#define PORT 9090
#define BUFFER_SIZE 1024
#define IDLE_TIMEOUT_SEC 10   // 이 시간 동안 아무 datagram도 없으면 나간 것으로 간주
#define SWEEP_INTERVAL_SEC 1  // select() 타임아웃 = 이 주기로 유휴 클라이언트를 검사

using namespace std;

// sockaddr_in을 "ip:port" 문자열로 바꿔 클라이언트 식별 키로 사용.
// UDP는 accept()가 없어서 fd 하나로 클라이언트를 구분하던 TCP와 달리, 소켓은 단 하나뿐이고
// recvfrom()이 돌려주는 (ip, port)로만 "누가 보냈는지"를 구분할 수 있음.
string addrKey(const sockaddr_in& addr) {
    // inet_ntoa는 내부 static 버퍼를 반환하지만(스레드-안전하지 않음), 이 프로그램은
    // 싱글스레드이고 반환 즉시 string으로 복사하므로 문제 없음.
    return string(inet_ntoa(addr.sin_addr)) + ":" + to_string(ntohs(addr.sin_port));
}

int main() {
    int sock_fd = socket(AF_INET, SOCK_DGRAM, 0); // TCP의 SOCK_STREAM 대신 SOCK_DGRAM
    if (sock_fd < 0) {
        perror("socket failed");
        return 1;
    }

    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(PORT);

    // UDP는 listen()/accept()가 없음 - bind()만 하면 이 포트로 오는 모든 datagram을 받을 준비 끝.
    // "연결을 맺는다"는 개념 자체가 없고, 그냥 주소를 정해서 소켓 하나로 누구든 보낸 datagram을 받는 것.
    if (bind(sock_fd, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        perror("bind failed");
        return 1;
    }

    cout << "UDP 서버가 포트 " << PORT << "에서 대기 중입니다...\n";

    deque<string> waiting_queue;      // 매칭 대기열 (TCP 버전과 동일한 이유로 deque)
    map<string, string> partner;      // key -> 매칭된 상대방 key
    map<string, sockaddr_in> addrOf;  // key -> 실제 sockaddr_in (sendto()에 필요)
    map<string, time_t> lastSeen;     // key -> 마지막으로 datagram을 받은 시각 (유휴 판정용)

    // key로 datagram 하나를 보냄. UDP는 TCP와 달리 커널 송신 버퍼가 꽉 차서 부분적으로만
    // 보내지는 경우가 없음(datagram은 통째로 보내지거나 실패하거나 둘 중 하나) -> TCP
    // 버전에서 만들었던 outbox/EAGAIN 버퍼링이 UDP에는 필요 없음.
    // 한 배치(이번 유휴 검사)에서 함께 정리될 예정인지 확인.
    // TCP 버전(server.cpp/server_epoll.cpp)의 "함정 2/3"과 정확히 같은 문제가 여기서도
    // 발생할 수 있음: 매칭된 두 클라이언트가 거의 동시에 타임아웃되면, 한쪽을 먼저 정리하며
    // 대기 중이던 제3자와 재매칭시켰다가 다른 한쪽을 정리하며 다시 풀어버리는 유령 매칭.
    // TCP에서 겪고 나서 얻은 교훈을 여기서는 처음부터 반영함(tryMatch보다 먼저 선언해야
    // tryMatch 안에서 캡처해 쓸 수 있음).
    vector<string> to_remove;
    auto isLeaving = [&](const string& key) {
        return find(to_remove.begin(), to_remove.end(), key) != to_remove.end();
    };

    auto sendMsg = [&](const string& key, const string& msg) {
        auto it = addrOf.find(key);
        if (it == addrOf.end()) return;
        if (sendto(sock_fd, msg.data(), msg.size(), 0,
                   (struct sockaddr*)&it->second, sizeof(it->second)) < 0) {
            perror("sendto failed");
        }
    };

    auto tryMatch = [&]() {
        // 대기열에 남아있지만 이번 배치에서 이미 끊기기로 정해진 key는 매칭 후보에서 제외.
        // (TCP server.cpp/server_epoll.cpp의 "함정 5"와 동일한 버그: Y-Z가 매칭되고 X가
        // 혼자 대기 중일 때, Y와 X가 같은 유휴 검사 배치에서 함께 타임아웃되면 X가 아직
        // waiting_queue에서 안 빠진 채로 tryMatch가 곧 사라질 X를 Z와 짝지어버렸다가,
        // 곧이어 X 자신의 removeClient()에서 그 매칭이 다시 풀리는 유령 매칭이 생김.
        // TCP에서 고쳤던 것과 같은 방식으로 여기도 포팅.)
        waiting_queue.erase(
            remove_if(waiting_queue.begin(), waiting_queue.end(), isLeaving),
            waiting_queue.end());

        while (waiting_queue.size() >= 2) {
            string a = waiting_queue.front(); waiting_queue.pop_front();
            string b = waiting_queue.front(); waiting_queue.pop_front();
            partner[a] = b;
            partner[b] = a;
            cout << "매칭 성사: " << a << " <-> " << b << "\n";
            sendMsg(a, "매칭 성사되었습니다! 상대방과 대화를 시작하세요.\n");
            sendMsg(b, "매칭 성사되었습니다! 상대방과 대화를 시작하세요.\n");
        }
    };

    auto removeClient = [&](const string& key) {
        auto qit = find(waiting_queue.begin(), waiting_queue.end(), key);
        if (qit != waiting_queue.end()) {
            waiting_queue.erase(qit);
        } else {
            auto it = partner.find(key);
            if (it != partner.end()) {
                string other = it->second;
                partner.erase(key);
                partner.erase(other);
                if (!isLeaving(other)) {
                    sendMsg(other, "상대방이 나갔습니다. 다시 매칭 대기 중입니다...\n");
                    waiting_queue.push_back(other);
                    tryMatch();
                }
            }
        }
        addrOf.erase(key);
        lastSeen.erase(key);
    };

    auto sweepIdle = [&]() {
        time_t now = time(nullptr);
        to_remove.clear();
        for (const auto& [key, seen] : lastSeen) {
            if (now - seen > IDLE_TIMEOUT_SEC) to_remove.push_back(key);
        }
        for (size_t k = 0; k < to_remove.size(); ++k) {
            const string& key = to_remove[k];
            if (addrOf.find(key) == addrOf.end()) continue; // 이미 정리됨
            cout << key << " - " << IDLE_TIMEOUT_SEC << "초간 응답이 없어 연결을 정리합니다.\n";
            removeClient(key);
        }
        to_remove.clear();
    };

    char buffer[BUFFER_SIZE];

    while (true) {
        fd_set read_fds;
        FD_ZERO(&read_fds);
        FD_SET(sock_fd, &read_fds);

        // TCP 버전에서는 항상 timeout=NULL(무한 대기)로 select를 불렀지만, 여기서는
        // "아무 데이터가 없어도 주기적으로 유휴 클라이언트를 검사"해야 하므로 유한한
        // timeout을 준다 - select()의 timeout 인자를 이 프로젝트에서 처음 실질적으로 사용.
        struct timeval tv;
        tv.tv_sec = SWEEP_INTERVAL_SEC;
        tv.tv_usec = 0;

        int ready = select(sock_fd + 1, &read_fds, NULL, NULL, &tv);
        if (ready < 0) {
            perror("select failed");
            break;
        }

        if (ready > 0 && FD_ISSET(sock_fd, &read_fds)) {
            struct sockaddr_in from_addr;
            socklen_t from_len = sizeof(from_addr);
            // MSG_TRUNC: datagram이 buffer보다 크면 넘치는 부분은 버려지지만, 반환값 n은
            // "잘리기 전 원래 datagram의 실제 크기"를 그대로 알려줌(리눅스 한정 동작).
            // 이걸로 n > sizeof(buffer)를 검사해서 잘림 여부를 알아낼 수 있음. 이 플래그
            // 없이는 recvfrom()이 항상 sizeof(buffer) 이하만 반환해서 잘렸는지 알 방법이 없음.
            ssize_t n = recvfrom(sock_fd, buffer, sizeof(buffer), MSG_TRUNC,
                                  (struct sockaddr*)&from_addr, &from_len);

            // UDP는 recvfrom()이 0을 반환해도 "연결 종료"가 아니라 그냥 0바이트짜리
            // datagram을 받은 것뿐임(TCP의 read()==0과 의미가 다름!). 클라이언트가 서버에게
            // 자기 존재를 처음 알릴 때 보내는 빈 datagram이 바로 이 케이스.
            if (n >= 0) {
                string key = addrKey(from_addr);
                bool isNew = (addrOf.find(key) == addrOf.end());
                addrOf[key] = from_addr; // 포트가 같아도 재접속 시 주소 정보를 최신으로 갱신
                lastSeen[key] = time(nullptr);

                if (isNew) {
                    cout << "새 클라이언트: " << key << "\n";
                    waiting_queue.push_back(key);
                    tryMatch();
                    if (!waiting_queue.empty() && waiting_queue.back() == key) {
                        sendMsg(key, "매칭 대기 중입니다...\n");
                    }
                }

                if (n > (ssize_t)sizeof(buffer)) {
                    // buffer에는 sizeof(buffer)만큼만 채워지고 나머지는 커널이 이미 버렸으므로,
                    // 잘린 내용을 그대로 relay하면 메시지가 깨진 채로 전달됨 -> 그냥 폐기.
                    // (연결 자체를 끊지는 않음 - 존재 확인/유휴 갱신은 위에서 이미 끝났고,
                    // 다음에 정상 크기 메시지를 보내면 그대로 계속 대화 가능)
                    cerr << key << " 이(가) " << sizeof(buffer) << "바이트를 넘는 datagram("
                         << n << "바이트)을 보내 잘렸습니다. 이 메시지는 폐기합니다.\n";
                } else if (n > 0) { // 0바이트는 접속 알림용이라 relay할 내용이 없음
                    // UDP는 datagram 단위로 도착하므로 recvfrom() 한 번 = sendto() 한 번과
                    // 정확히 대응됨. TCP처럼 메시지가 잘리거나 여러 개가 붙어서 오는 문제
                    // 자체가 없어서 inbox 버퍼링이나 개행 프레이밍이 필요 없음.
                    string line(buffer, n);
                    cout << "[" << key << "] 수신: " << line;

                    auto it = partner.find(key);
                    if (it != partner.end()) {
                        sendMsg(it->second, line);
                    } else {
                        sendMsg(key, "아직 매칭 대기 중입니다. 잠시만 기다려주세요.\n");
                    }
                }
            } else if (errno != EAGAIN && errno != EWOULDBLOCK) {
                perror("recvfrom failed");
            }
        }

        // select가 timeout으로 깨어났든 datagram을 처리했든, 매 루프마다 유휴 클라이언트를 검사
        sweepIdle();
    }

    close(sock_fd);
    cout << "서버를 종료합니다.\n";
    return 0;
}

#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <sys/socket.h>
#include <arpa/inet.h>

#define SERV_IP "0.0.0.0"
#define SERV_PORT 80

/*client*/
int main(int argc, char **argv){
    int i = 0;
    for (i = 0; i < 90; i++) {
        int        sockfd;
        struct linger     ling;
        struct sockaddr_in    servaddr;
 
        //printf("0111111111111\n");
        sockfd = socket(AF_INET, SOCK_STREAM, 0);
        bzero(&servaddr, sizeof(servaddr));
        servaddr.sin_family = AF_INET;
        servaddr.sin_port = htons(SERV_PORT);
        inet_pton(AF_INET, SERV_IP, &servaddr.sin_addr.s_addr);
        connect(sockfd, (struct sockaddr*)&servaddr, sizeof(servaddr));
        
        //printf("1111111111111\n");
        ling.l_onoff = 1;
        ling.l_linger = 0;
        setsockopt(sockfd, SOL_SOCKET, SO_LINGER, &ling, sizeof(ling));
        close(sockfd);
        //printf("2111111111111\n");
    }

    return 0;
}


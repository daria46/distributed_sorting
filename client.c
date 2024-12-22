#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/signal.h>
#include <arpa/inet.h>
#include <sys/select.h>
#include <fcntl.h>
#include <string.h>

#define RCVPORT 38199

// Структура для отправки задания серверу
typedef struct {
    int *array;
    int left;
    int right;
} task_data_t;

int array_size_global;

// Аргумент для треда работающего с сервером
typedef struct {
    int left;   // Левая граница
    int right;  // Правая граница
    int *array;
    struct sockaddr_in *server;  // Структура с информацией для подключения к серверу
    int *sorted_array;  // Куда записать результат
} thread_args_t;

// Функция треда работающего с сервером
void *send_thread(void *arg) {
    thread_args_t *task_data = (thread_args_t *)arg;
    int servsock = socket(PF_INET, SOCK_STREAM, 0);
    if (servsock < 0) {
        perror("Create new socket to server");
        exit(EXIT_FAILURE);
    }

    if (connect(servsock, (struct sockaddr *)task_data->server, sizeof(struct sockaddr_in)) < 0) {
        perror("Connect to server failed!");
        exit(EXIT_FAILURE);
    }

    if (send(servsock, &array_size_global, sizeof(int), 0) < 0) {
        perror("Sending array size to server failed");
        exit(EXIT_FAILURE);
    }
    if (send(servsock, &task_data->left, sizeof(int), 0) < 0) {
        perror("Sending left to server failed");
        exit(EXIT_FAILURE);
    }
    if (send(servsock, &task_data->right, sizeof(int), 0) < 0) {
        perror("Sending right to server failed");
        exit(EXIT_FAILURE);
    }

    int sent_bytes = send(servsock, task_data->array, sizeof(int) * (task_data->right - task_data->left + 1), 0);
    if (sent_bytes < 0) {
        perror("Sending array to server failed");
        exit(EXIT_FAILURE);
    }

    int *buf = malloc(sizeof(int) * (task_data->right - task_data->left + 1));
    int recv_byte = recv(servsock, buf, sizeof(int) * (task_data->right - task_data->left + 1), 0);
    if (recv_byte == 0) {
        fprintf(stderr, "Server %s on port %d die!\nCancel calculate, on all",
                inet_ntoa(task_data->server->sin_addr),
                ntohs(task_data->server->sin_port));
        exit(EXIT_FAILURE);
    }

    memcpy(&task_data->sorted_array[task_data->left], buf, sizeof(int) * (task_data->right - task_data->left + 1));
    free(buf);
    close(servsock);

    return NULL;
}

// Функция для слияния двух отсортированных подмассивов
void merge(int arr[], int l, int m, int r) {
    int i, j, k;
    int n1 = m - l + 1;
    int n2 = r - m;

    int L[n1], R[n2];

    for (i = 0; i < n1; i++) L[i] = arr[l + i];
    for (j = 0; j < n2; j++) R[j] = arr[m + 1 + j];

    i = 0;
    j = 0;
    k = l;
    while (i < n1 && j < n2) {
        if (L[i] <= R[j]) {
            arr[k] = L[i];
            i++;
        } else {
            arr[k] = R[j];
            j++;
        }
        k++;
    }

    while (i < n1) {
        arr[k] = L[i];
        i++;
        k++;
    }

    while (j < n2) {
        arr[k] = R[j];
        j++;
        k++;
    }
}

// Функция для объединения отсортированных подмассивов
void merge_sorted_arrays(int arr[], int left[], int right[], int size) {
    for (int i = 0; i < size - 1; i++) {
        merge(arr, left[i], right[i], right[i + 1]);
    }
}

int main(int argc, char **argv) {
    int maxservu;
    int array_size;

    printf("Enter array size: ");
    scanf("%d", &array_size);
    int *array = malloc(sizeof(int) * array_size);
    printf("Enter array: ");
    for (int i = 0; i < array_size; i++) {
        scanf("%d", &array[i]);
    }
    array_size_global = array_size;
    printf("Enter max servers: ");
    scanf("%d", &maxservu);

    int sockbrcast = socket(PF_INET, SOCK_DGRAM, 0);
    if (sockbrcast == -1) {
        perror("Create broadcast socket failed");
        exit(EXIT_FAILURE);
    }

    int port_rcv = 0;
    struct sockaddr_in addrbrcast_rcv;
    bzero(&addrbrcast_rcv, sizeof(addrbrcast_rcv));
    addrbrcast_rcv.sin_family = AF_INET;
    addrbrcast_rcv.sin_addr.s_addr = htonl(INADDR_ANY);
    addrbrcast_rcv.sin_port = 0;
    if (bind(sockbrcast, (struct sockaddr *)&addrbrcast_rcv, sizeof(addrbrcast_rcv)) < 0) {
        perror("Bind broadcast socket failed");
        close(sockbrcast);
        exit(EXIT_FAILURE);
    }

    int port_snd = RCVPORT;
    struct sockaddr_in addrbrcast_snd;
    bzero(&addrbrcast_snd, sizeof(addrbrcast_snd));
    addrbrcast_snd.sin_family = AF_INET;
    addrbrcast_snd.sin_port = htons(port_snd);
    addrbrcast_snd.sin_addr.s_addr = htonl(INADDR_BROADCAST);

    int access = 1;
    if (setsockopt(sockbrcast, SOL_SOCKET, SO_BROADCAST, (const void *)&access, sizeof(access)) < 0) {
        perror("Can't accept broadcast option at socket to send");
        close(sockbrcast);
        exit(EXIT_FAILURE);
    }
    int msgsize = sizeof(char) * 18;
    void *hellomesg = malloc(msgsize);
    bzero(hellomesg, msgsize);
    strcpy(hellomesg, "Hello sorting");
    if (sendto(sockbrcast, hellomesg, msgsize, 0, (struct sockaddr *)&addrbrcast_snd, sizeof(addrbrcast_snd)) < 0) {
        perror("Sending broadcast");
        close(sockbrcast);
        exit(EXIT_FAILURE);
    }

    fcntl(sockbrcast, F_SETFL, O_NONBLOCK);

    fd_set readset;
    FD_ZERO(&readset);
    FD_SET(sockbrcast, &readset);

    struct timeval timeout;
    timeout.tv_sec = 3;
    timeout.tv_usec = 0;

    struct sockaddr_in *servers = (struct sockaddr_in *)malloc(sizeof(struct sockaddr_in));
    bzero(servers, sizeof(struct sockaddr_in));
    int servcount = 0;
    int maxserv = 1;
    socklen_t servaddrlen = sizeof(struct sockaddr_in);
    while (select(sockbrcast + 1, &readset, NULL, &readset, &timeout) > 0) {
        int rdbyte = recvfrom(sockbrcast, (void *)hellomesg, msgsize, MSG_TRUNC, (struct sockaddr *)&servers[servcount], &servaddrlen);
        if (rdbyte == msgsize && strcmp(hellomesg, "Hello Client") == 0) {
            servcount++;
            if (servcount >= maxserv) {
                servers = realloc(servers, sizeof(struct sockaddr_in) * (maxserv + 1));
                if (servers == NULL) {
                    perror("Realloc failed");
                    close(sockbrcast);
                    exit(EXIT_FAILURE);
                }
                bzero(&servers[servcount], servaddrlen);
                maxserv++;
            }
            FD_ZERO(&readset);
            FD_SET(sockbrcast, &readset);
        }
    }
    int i;
    if (servcount < 1) {
        fprintf(stderr, "No servers found!\n");
        exit(EXIT_FAILURE);
    }
    if (argc > 3 && maxservu <= servcount) servcount = maxservu;
    for (i = 0; i < servcount; ++i) {
        printf("Server answer from %s on port %d\n", inet_ntoa(servers[i].sin_addr), ntohs(servers[i].sin_port));
    }
    printf("\n");
    free(hellomesg);

    int *sorted_array = (int *)malloc(sizeof(int) * array_size);
    pthread_t *tid = (pthread_t *)malloc(sizeof(pthread_t) * servcount);
    int subarray_size = array_size / servcount;
    int remaining_elements = array_size % servcount;
    int *left = (int *)malloc(sizeof(int) * servcount);
    int *right = (int *)malloc(sizeof(int) * servcount);

    for (i = 0; i < servcount; ++i) {
        thread_args_t *args = (thread_args_t *)malloc(sizeof(thread_args_t));
        args->left = i * subarray_size;
        args->right = args->left + subarray_size - 1;
        if (i == servcount - 1) {
            args->right += remaining_elements;
        }
        args->array = (int *)malloc(sizeof(int) * (args->right - args->left + 1));
        memcpy(args->array, &array[args->left], sizeof(int) * (args->right - args->left + 1));
        args->server = &servers[i];
        args->sorted_array = sorted_array;
        left[i] = args->left;
        right[i] = args->right;
        if (pthread_create(&tid[i], NULL, send_thread, args) != 0) {
            perror("Create send thread failed");
            exit(EXIT_FAILURE);
        }
    }

    for (i = 0; i < servcount; ++i) {
        pthread_join(tid[i], NULL);
    }

    merge_sorted_arrays(sorted_array, left, right, servcount);

    printf("Sorted array:\n");
    for (int i = 0; i < array_size; ++i) {
        printf("%d ", sorted_array[i]);
    }
    printf("\n");

    free(servers);
    free(sorted_array);
    free(tid);
    free(left);
    free(right);

    return (EXIT_SUCCESS);
}

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
#define BUFFER_SIZE  1024

// Структура для передачи задания серверу
typedef struct {
    int *array;  // Массив для сортировки
    int left;    // Левый индекс подмассива
    int right;   // Правый индекс подмассива
} task_data_t;

// Аргумент функции треда вычислителя
typedef struct {
    int *array;  // Массив для сортировки
    int left;    // Левый индекс подмассива
    int right;   // Правый индекс подмассива
} thread_args_t;

// Функция для обмена двух элементов
void swap(int *a, int *b) {
    int temp = *a;
    *a = *b;
    *b = temp;
}

// Функция для "просеивания" элемента вниз в куче
void heapify(int *array, int n, int i) {
    int largest = i; // Инициализируем наибольший элемент как корень
    int left = 2 * i + 1; // Левый потомок
    int right = 2 * i + 2; // Правый потомок

    // Если левый потомок больше корня
    if (left < n && array[left] > array[largest])
        largest = left;

    // Если правый потомок больше самого большого элемента на данный момент
    if (right < n && array[right] > array[largest])
        largest = right;

    // Если наибольший элемент не корень
    if (largest != i) {
        swap(&array[i], &array[largest]);

        // Рекурсивно просеиваем поддерево
        heapify(array, n, largest);
    }
}

// Основная функция пирамидальной сортировки
void heapSort(int *array, int n) {
    // Построение кучи (перегруппировка массива)
    for (int i = n / 2 - 1; i >= 0; i--)
        heapify(array, n, i);

    // Один за другим извлекаем элементы из кучи
    for (int i = n - 1; i > 0; i--) {
        // Перемещаем текущий корень в конец
        swap(&array[0], &array[i]);

        // Вызываем процедуру heapify на уменьшенной куче
        heapify(array, i, 0);
    }
}

void merge(int *array, int left, int mid, int right) {
    int n1 = mid - left + 1;
    int n2 = right - mid;

    int *L = (int *)malloc(n1 * sizeof(int));
    int *R = (int *)malloc(n2 * sizeof(int));

    for (int i = 0; i < n1; i++)
        L[i] = array[left + i];
    for (int j = 0; j < n2; j++)
        R[j] = array[mid + 1 + j];

    int i = 0, j = 0, k = left;
    while (i < n1 && j < n2) {
        if (L[i] <= R[j]) {
            array[k] = L[i];
            i++;
        } else {
            array[k] = R[j];
            j++;
        }
        k++;
    }

    while (i < n1) {
        array[k] = L[i];
        i++;
        k++;
    }

    while (j < n2) {
        array[k] = R[j];
        j++;
        k++;
    }

    free(L);
    free(R);
}
// Функция треда вычислителя
void *calculate(void *arg) {
    // Ожидаем в качестве аргумента указатель на структуру thread_args_t
    thread_args_t *tinfo = (thread_args_t *)arg;
    int *array = tinfo->array;
    int left = tinfo->left;
    int right = tinfo->right;

    if (right - left < 1000) {
        heapSort(array + left, right - left + 1);
    }
    else if (left < right) {
        int m = left + (right - left) / 2;

        // Создаем аргументы для левого и правого тредов
        thread_args_t left_args = {array, left, m};
        thread_args_t right_args = {array, m + 1, right};

        // Создаем левый и правый треды
        pthread_t left_thread, right_thread;
        pthread_create(&left_thread, NULL, calculate, (void *)&left_args);
        pthread_create(&right_thread, NULL, calculate, (void *)&right_args);

        // Ждем завершения левого и правого тредов
        pthread_join(left_thread, NULL);
        pthread_join(right_thread, NULL);

        // Сливаем отсортированные левый и правый подмассивы
        merge(array, left, m, right);
    }
    pthread_exit(NULL);
}

// Аргумент для проверяющего клиента треда
typedef struct {
    int sock;  // Сокет с клиентом
    pthread_t *calcthreads;  // Треды которые в случае чего надо убить
    int threadnum;  // Количество этих тредов
} checker_args_t;

// Функция которая будет выполнена тредом получившим сигнал SIGUSR1
void thread_cancel(int signo) { pthread_exit(PTHREAD_CANCELED); }

// Тред проверяющий состояние клиента
void *client_check(void *arg) {
    // Нам должен быть передан аргумент типа checker_args_t
    checker_args_t *args = (checker_args_t *)arg;
    char a[10];
    recv(args->sock, &a, 10, 0);  // Так как мы используем TCP, если клиент умрет или что либо
    // скажет, то recv тут же разблокирует тред и вернёт -1
    int st;
    for (int i = 0; i < args->threadnum; ++i)
        st = pthread_kill(args->calcthreads[i], SIGUSR1);  // Шлем всем вычислителям SIGUSR1
    return NULL;
}

void *listen_broadcast(void *arg) {
    int *isbusy = arg;
    // Создаем сокет для работы с broadcast
    int sockbrcast = socket(PF_INET, SOCK_DGRAM, 0);
    if (sockbrcast == -1) {
        perror("Create broadcast socket failed");
        exit(EXIT_FAILURE);
    }

    // Создаем структуру для приема ответов на broadcast
    int port_rcv = RCVPORT;
    struct sockaddr_in addrbrcast_rcv;
    bzero(&addrbrcast_rcv, sizeof(addrbrcast_rcv));
    addrbrcast_rcv.sin_family = AF_INET;
    addrbrcast_rcv.sin_addr.s_addr = htonl(INADDR_ANY);
    addrbrcast_rcv.sin_port = htons(port_rcv);
    // Биндим её
    if (bind(sockbrcast, (struct sockaddr *)&addrbrcast_rcv, sizeof(addrbrcast_rcv)) < 0) {
        perror("Bind broadcast socket failed");
        close(sockbrcast);
        exit(EXIT_FAILURE);
    }

    int msgsize = sizeof(char) * 18;
    char hellomesg[18];
    bzero(hellomesg, msgsize);
    // Делаем прослушивание сокета broadcast'ов неблокирующим
    fcntl(sockbrcast, F_SETFL, O_NONBLOCK);

    // Создаем множество прослушивания
    fd_set readset;
    FD_ZERO(&readset);
    FD_SET(sockbrcast, &readset);

    // Таймаут
    struct timeval timeout;
    timeout.tv_sec = 3;
    timeout.tv_usec = 0;

    struct sockaddr_in client;
    bzero(&client, sizeof(client));
    socklen_t servaddrlen = sizeof(struct sockaddr_in);
    char helloanswer[18];
    bzero(helloanswer, msgsize);
    strcpy(helloanswer, "Hello Client");
    int sockst = 1;
    while (sockst > 0) {
        sockst = select(sockbrcast + 1, &readset, NULL, &readset, NULL);
        if (sockst == -1) {
            perror("Broblems on broadcast socket");
            exit(EXIT_FAILURE);
        }
        int rdbyte = recvfrom(sockbrcast, (void *)hellomesg, msgsize, MSG_TRUNC, (struct sockaddr *)&client, &servaddrlen);
        if (rdbyte == msgsize && strcmp(hellomesg, "Hello sorting") == 0 && *isbusy == 0) {
            if (sendto(sockbrcast, helloanswer, msgsize, 0, (struct sockaddr *)&client, sizeof(struct sockaddr_in)) < 0) {
                perror("Sending answer");
                close(sockbrcast);
                exit(EXIT_FAILURE);
            }
        }
        FD_ZERO(&readset);
        FD_SET(sockbrcast, &readset);
    }
    return NULL;
}

int main(int argc, char **argv) {
    // Аргумент может быть только один - это кол-во тредов
    if (argc > 2) {
        fprintf(stderr, "Usage: %s [numofcpus]\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    int numofthread;
    if (argc == 2) {
        numofthread = atoi(argv[1]);
        if (numofthread < 1) {
            fprintf(stderr, "Incorrect num of threads!\n");
            exit(EXIT_FAILURE);
        }
        fprintf(stdout, "Num of threads forced to %d\n", numofthread);
    } else {
        // Если аргументов нет, то определяем кол-во процессоров автоматически
        numofthread = sysconf(_SC_NPROCESSORS_ONLN);
        if (numofthread < 1) {
            fprintf(stderr, "Can't detect num of processors\n" "Continue in two threads\n");
            numofthread = 2;
        }
        fprintf(stdout, "Num of threads detected automatically it's %d\n\n", numofthread);
    }

    struct sigaction cancel_act;
    memset(&cancel_act, 0, sizeof(cancel_act));
    cancel_act.sa_handler = thread_cancel;
    sigfillset(&cancel_act.sa_mask);
    sigaction(SIGUSR1, &cancel_act, NULL);

    // Создаем тред слушающий broadcast'ы
    pthread_t broadcast_thread;
    int isbusy = 1;  // Переменная которая сообщает треду следует ли отвечать на broadcast
    if (pthread_create(&broadcast_thread, NULL, listen_broadcast, &isbusy)) {
        fprintf(stderr, "Can't create broadcast listen thread");
        perror("Detail:");
        exit(EXIT_FAILURE);
    }

    int listener;
    struct sockaddr_in addr;
    listener = socket(PF_INET, SOCK_STREAM, 0);
    if (listener < 0) {
        perror("Can't create listen socket");
        exit(EXIT_FAILURE);
    }

    addr.sin_family = AF_INET;
    addr.sin_port = htons(RCVPORT);
    addr.sin_addr.s_addr = INADDR_ANY;
    int a = 1;
    // Добавляем опцию SO_REUSEADDR для случаев когда мы перезапускам сервер
    if (setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, &a, sizeof(a)) < 0) {
        perror("Set listener socket options");
        exit(EXIT_FAILURE);
    }

    // Биндим сокет
    if (bind(listener, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("Can't bind listen socket");
        exit(EXIT_FAILURE);
    }

    // Начинаем ждать соединения от клиентов
    if (listen(listener, 1) < 0) {
        perror("Eror listen socket");
        exit(EXIT_FAILURE);
    }

    // Ожидаем соединений
    int needexit = 0;
    while (needexit == 0) {
        fprintf(stdout, "\nWait new connection...\n\n");
        int client;
        isbusy = 0;  // Разрешаем отвечать клиентам на запросы
        struct sockaddr_in addrclient;
        socklen_t addrclientsize = sizeof(addrclient);
        client = accept(listener, (struct sockaddr *)&addrclient, &addrclientsize);
        if (client < 0) {
            perror("Client accepting");
        }
        isbusy = 1;  // Запрещаем отвечать на запросы

        int array_size = 0;
        recv(client, &array_size, sizeof(int), 0);
        int left, right;
        recv(client, &left, sizeof(int), 0);
        printf("left: %d\n", left);
        recv(client, &right, sizeof(int), 0);
        printf("right: %d\n", right);
        printf("array_size: %d\n", array_size);

        int *buf = (int *)malloc(sizeof(int) * array_size);
        int received, total_received = 0;

        while (total_received < array_size*sizeof(int)) {
            received = recv(client, buf + total_received / sizeof(int), sizeof(int) * BUFFER_SIZE, 0);
            if (received < 0) {
                perror("Receiving data from client");
                exit(EXIT_FAILURE);
            }
            total_received += received;
            printf("bytes_received: %d\n", received);
        }
        printf("total received: %d\n", total_received);
        if (total_received != array_size * sizeof(int) || left < 0 || right < 0) {
            fprintf(stderr, "Invalid data from %s on port %d, reset peer\n",
                    inet_ntoa(addrclient.sin_addr), ntohs(addrclient.sin_port));
            close(client);
            isbusy = 0;
        } else {
            fprintf(stdout, "Calculate and send to %s on port %d\n",
                    inet_ntoa(addrclient.sin_addr), ntohs(addrclient.sin_port));
            task_data_t data;
            data.array = buf;
            data.left = left;
            data.right = right;
            pthread_t thread;
            pthread_create(&thread, NULL, calculate, &data);
            pthread_join(thread, NULL);

            int sent = send(client, buf, sizeof(int) * array_size, 0);
            printf("sent: %d\n", sent);
            close(client);
            isbusy = 0;
            fprintf(stdout, "Calculate and send finish!\n");
        }
        free(buf);
    }

    return (EXIT_SUCCESS);
}
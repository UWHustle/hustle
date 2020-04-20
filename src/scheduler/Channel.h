#ifndef HUSTLE_SRC_SCHEDULER_CHANNEL_H_
#define HUSTLE_SRC_SCHEDULER_CHANNEL_H_

#include <iostream>
#include <string>
#include <thread>
#include <nng/nng.h>
#include <nng/protocol/pipeline0/pull.h>
#include <nng/protocol/pipeline0/push.h>

namespace hustle::scheduler {

static void fatal(const char *func, int rv) {
  fprintf(stderr, "%s: %s\n", func, nng_strerror(rv));
  exit(1);
}

template <typename T>
class Sender {
 public:
  explicit Sender(std::string addr)
      : addr_(std::move(addr)), sender_sock_() {
    start();
  }

  ~Sender() {
    nng_close(sender_sock_);
  }

  void send(T* msg) {
    int rv;

    if ((rv = nng_send(sender_sock_, msg, sizeof(msg), 0)) != 0) {
      fatal("nng_send", rv);
    }

    printf("SENT: %p, %s\n", msg, msg->toString().c_str());

    // wait for messages to flush before shutting down
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  /**
   * Clone a new sender with the same addr_
   * @return std::shared_ptr<Sender<T>> sender
   */
  std::unique_ptr<Sender<T>> clone() {
    return std::make_unique<Sender<T>>(addr_);
  }

  void start() {
    int rv;
    if ((rv = nng_push0_open(&sender_sock_)) != 0) {
      fatal("nng_push0_open", rv);
    }
    if ((rv = nng_dial(sender_sock_, addr_.c_str(), nullptr, NNG_FLAG_NONBLOCK))
        != 0) {
      fatal("nng_dial", rv);
    }
  }

 private:
  std::string addr_;
  nng_socket sender_sock_;
};


template <typename T>
class Receiver {
 public:
  explicit Receiver(std::string addr)
      : addr_(std::move(addr)), receiver_sock_() {
    start();
  }

  ~Receiver() {
    nng_close(receiver_sock_);
  }


  T* receive() {
    int rv;
    void *buf = nullptr;
    size_t sz;

    if ((rv = nng_recv(receiver_sock_, &buf, &sz, NNG_FLAG_ALLOC)) != 0) {
      fatal("nng_recv", rv);
    }
    auto* msg = (T*) buf;
    nng_free(buf, sz);

    return msg;
  }

  void start() {
    int rv;

    if ((rv = nng_pull0_open(&receiver_sock_)) != 0) {
      fatal("nng_pull0_open", rv);
    }
    if ((rv = nng_listen(receiver_sock_, addr_.c_str(), nullptr, 0)) != 0) {
      fatal("nng_listen", rv);
    }
  }

 private:
  std::string addr_;
  nng_socket receiver_sock_;
};

template <typename T>
std::pair<std::unique_ptr<Sender<T>>,
          std::unique_ptr<Receiver<T>>> channel(const std::string &addr) {
  auto receiver = std::make_unique<Receiver<T>>(addr);
  auto sender = std::make_unique<Sender<T>>(addr);
  return std::make_pair(std::move(sender), std::move(receiver));
}


}

#endif //HUSTLE_SRC_SCHEDULER_CHANNEL_H_

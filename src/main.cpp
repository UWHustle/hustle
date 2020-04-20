#include <thread>
#include <vector>
#include <scheduler/Block.h>
#include "scheduler/Channel.h"

#define NUM_THREADS 1000

template <typename T>
void send(std::unique_ptr<hustle::scheduler::Sender<T>> sender, int num) {
  // std::this_thread::sleep_for(std::chrono::milliseconds(rand() % 100));

  std::shared_ptr<T> msg = std::make_shared<T>(num);
  sender->send(msg);

  /// wait until receiver has received the msg. (try to comment out)
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));
}

template <typename T>
void recv(std::unique_ptr<hustle::scheduler::Receiver<T>> receiver) {
  // std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  for (int i = 0; i < NUM_THREADS; i++) {
    auto block = receiver->receive();
    printf("RECEIVED: %s\n", block->toString().c_str());
  }
}

int main() {
  std::string addr = "inproc://test";
  std::vector<std::thread> vec;
  typedef Block T;
  auto [sender, receiver] = hustle::scheduler::channel<T>(addr);

  for (int i = 1; i <= NUM_THREADS - 1; i++) {
    vec.push_back(std::move(std::thread(send<T>, std::move(sender->clone()), i)));
  }
  vec.push_back(std::move(std::thread(send<T>, std::move(sender), NUM_THREADS)));
  vec.push_back(std::move(std::thread(recv<T>, std::move(receiver))));

  for (auto & thread : vec) {
    thread.join();
  }

  return 0;
}


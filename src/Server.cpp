#include <iostream>
#include<chrono>
#include <cstdlib>
#include <string>
#include <cstring>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <arpa/inet.h>
#include <netdb.h>

#include<vector>
#include<unordered_map>

const int MAX{1024};
const int BUFFERSIZE{1024};

std::unordered_map<std::string, std::string> kvstore;
std::unordered_map<std::string, uint64_t> kvstore_expiries;


std::vector<std::string> tokenize(const std::string& str, const std::string& delimiter) {
    std::vector<std::string> tokens;
    size_t start = 0, end;
    
    while ((end = str.find(delimiter, start)) != std::string::npos) {
        tokens.push_back(str.substr(start, end - start));
        start = end + delimiter.length();
    }
    
    // Add the last token if any
    if (start < str.length()) {
        tokens.push_back(str.substr(start));
        
    }

    for(auto tt : tokens)
    {
      std::cout << "tt = " << tt << std::endl;
    }
    
    return tokens;
}

bool compareStrings(std::string str1, std::string str2)
{
    if (str1.size() != str2.size())
        return false;

    for (int i = 0; i < str1.size(); ++i) {
        if (std::tolower(str1[i]) != std::tolower(str2[i]))
            return false;
    }

    return true;
}

int handleClientResponse(int client_fd)
{
  char buffer[BUFFERSIZE];
  int n = read(client_fd, buffer, BUFFERSIZE-1);

  if(n < 0)
  {
    std::cout << "cllient disconnected while reading , n = " << n << std::endl;
    return n;
  }
  else if(n == 0)
  {
    std::cout << "cllient didn't send any data while reading , n = " << n << std::endl;
    return 0;
  }

  buffer[n] = '\0'; // null terminate the string
  std::string client_send = std::string(buffer);
  std::cout << "received from client = " << client_send << std::endl;
  std::cout << "received from client size = = " << client_send.size() << std::endl;

  switch(client_send[0])
  {
    case '+':
      std::cout << "simple string" << std::endl;
      break;
    case '-':
      std::cout << "simple error" << std::endl;
      break;
    case ':':
      std::cout << "simple integer" << std::endl;
      break;
    case '$':
      std::cout << "bulk string" << std::endl;
      break;
    case '*':
    {
      std::cout << "array " << std::endl;
      auto tokens = tokenize(client_send, "\r\n");

      int num_elements = std::stoi(tokens[0].substr(1));
      std::cout << "num_elemnts = " << num_elements << std::endl;
      std::vector<std::string> strs_received;
      for(int i = 2; i < tokens.size(); i+=2)
      {
        strs_received.push_back(tokens[i]);
      }


      int ci = 0;
      while(ci < strs_received.size())
      {
        std::string strs = strs_received[ci];
        std::cout << "strs = " << strs << std::endl;
        if(compareStrings(strs, "PING"))
        {
          std::string res = "+PONG\r\n";
          write(client_fd, res.c_str(), res.size());
        }
        else if(compareStrings(strs, "ECHO"))
        {
          // get next string
          ci++; // goto that string
          std::string res = "+" + strs_received[ci] + "\r\n";
          write(client_fd, res.c_str(), res.size());
        }
        else if(compareStrings(strs, "SET"))
        {
          // get key string
          ci++; // goto that string
          std::string key = strs_received[ci];
          //get val string
          ci++;
          std::string val = strs_received[ci];
          std::cout << "key = " << key << " , val = " << val << std::endl;
          kvstore[key] = val;

          if(ci < strs_received.size())
          {
            ci++; // goto opiton
            std::string option = strs_received[ci];
            if(ci < n && compareStrings(option, "px"))
            {
              ci++; // goto time val
              uint64_t ttl = std::atol(strs_received[ci].c_str());
              uint64_t current_epoch = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()
              ).count();
              uint64_t expiry_epoch = current_epoch + ttl; 
              std::cout << "setting expiry key = " << key << " , epoch = " << expiry_epoch << std::endl;
              kvstore_expiries[key] = expiry_epoch;
            }
          }

          std::string res = "+OK\r\n";
          write(client_fd, res.c_str(), res.size());
        }
        else if(compareStrings(strs, "GET"))
        {
          // get key string
          ci++; // goto that string
          std::string key = strs_received[ci];
          //get val string from kvstore
          std::string res = "";
          if(kvstore.find(key) != kvstore.end())
          {
              uint64_t current_epoch = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()
              ).count();

              if(current_epoch  <= kvstore_expiries[key])
              {
                std::string val = kvstore[key];
                res = "$" + std::to_string(val.size()) + "\r\n" + val + "\r\n";
              }
              else
              {
                res = "$-1\r\n";
              }
          }
          else
            res = "$-1\r\n";

          std::cout << "res = " << res << std::endl;
          write(client_fd, res.c_str(), res.size());
        }
        else
        {
          std::cout << "new command = " << strs << std::endl; 
          std::string res = "+newcommand\r\n";
          write(client_fd, res.c_str(), res.size());
        }


        ci++;
      }
    }
      break;
    case '_':
      std::cout << "null recievd" << std::endl;
      break;
    case '#':
      std::cout << "recived bool " << std::endl;
      break;
    case ',':
      std::cout << "recieved double" << std::endl;
      break;
    case '(':
      std::cout << "received big number" << std::endl;
      break;
    case '!':
      std::cout << "bulk errors" << std::endl;
      break;
    case '=':
      std::cout << "verbatim string" << std::endl;
      break;
    case '%':
      std::cout << "maps recived" << std::endl;
      break;
    case '`':
      std::cout << "attributes recived" << std::endl;
      break;
    case '~':
      std::cout << "sets recived" << std::endl;
      break;
    case '>':
      std::cout << "pushes recived" << std::endl;
      break;
    default:
      std::cout << "unknown RESP redis character." << std::endl;
      break;
  }


  

  return 99;
}

int main(int argc, char **argv) {
  // Flush after every std::cout / std::cerr
  std::cout << std::unitbuf;
  std::cerr << std::unitbuf;
  
  int server_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (server_fd < 0) {
   std::cerr << "Failed to create server socket\n";
   return 1;
  }
  
  // Since the tester restarts your program quite often, setting SO_REUSEADDR
  // ensures that we don't run into 'Address already in use' errors
  int reuse = 1;
  if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
    std::cerr << "setsockopt failed\n";
    return 1;
  }
  
  struct sockaddr_in server_addr;
  server_addr.sin_family = AF_INET;
  server_addr.sin_addr.s_addr = INADDR_ANY;
  server_addr.sin_port = htons(6379);
  
  if (bind(server_fd, (struct sockaddr *) &server_addr, sizeof(server_addr)) != 0) {
    std::cerr << "Failed to bind to port 6379\n";
    return 1;
  }
  
  int connection_backlog = 5;
  if (listen(server_fd, connection_backlog) != 0) {
    std::cerr << "listen failed\n";
    return 1;
  }

  int epoll_fd = epoll_create1(0);
  if(epoll_fd < 0)
  {
    std::cerr << "epoll creation fail" << std::endl;
    return -1;
  }

  struct epoll_event ev;

  std::cout << "putting in stdin in epoll " << std::endl;
  ev.events = EPOLLIN;
  ev.data.fd = STDIN_FILENO;
  if(epoll_ctl(epoll_fd, EPOLL_CTL_ADD, STDIN_FILENO, &ev) < 0)
  {
    std::cerr << "error while adding stdin to epoll" << std::endl;
    return -1;
  }

  std::cout << "putting in server_fd in epooll" << std::endl;
  ev.events = EPOLLIN;
  ev.data.fd = server_fd;
  if(epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_fd, &ev) < 0)
  {
    std::cerr << "error while adding server_fd to epoll" << std::endl;
    return -1;
  }

  while(true)
  {
    struct epoll_event evlist[MAX];
    int number_of_ready_fds = epoll_wait(epoll_fd, evlist, MAX, -1);
    std::cout << "number_of_ready_fds = " << number_of_ready_fds << std::endl;

    if(number_of_ready_fds < 0)
    {
      std::cerr << "error in epoll_wait" << std::endl;
      break;
    }

    for(int i = 0; i < number_of_ready_fds; i++)
    {
      struct epoll_event epv = evlist[i];

      if(epv.events & EPOLLIN)
      {
        if(epv.data.fd == STDIN_FILENO)
        {
          std::cout << "it is stdin" << std::endl;
          std::string line;
          getline(std::cin, line);
          std::cout << "stdin has = " << line << std::endl;
          if(line == "quit" || line == "exit")
          {
            close(server_fd);
            close(epoll_fd);
            return 10;
          }
        }
        else if(epv.data.fd == server_fd)
        {
          std::cout << "it is server" << std::endl;
          struct sockaddr_in client_addr;
          int client_addr_len = sizeof(client_addr);
          std::cout << "Waiting for a client to connect...\n";

          // You can use print statements as follows for debugging, they'll be visible when running tests.
          std::cout << "Logs from your program will appear here!\n";

          int client_fd = accept(server_fd, (struct sockaddr *) &client_addr, (socklen_t *) &client_addr_len);
          if(client_fd < 0)
          {
            std::cerr << "failed to accept client" << std::endl;
          }
          std::cout << "Client connected, putting it in epoll intereest list" << std::endl;
          struct epoll_event client_connect_event;
          client_connect_event.events = EPOLLIN;
          client_connect_event.data.fd = client_fd;
          if(epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &client_connect_event) < 0)
          {
            std::cerr << "error while adding server_fd to epoll" << std::endl;
            return -1;
          }
        }
        else
        {
          std::cout << "it is client" << std::endl;
          int client_fd = epv.data.fd;
          int res = handleClientResponse(client_fd);

          if(res <= 0)
          {
            if(epoll_ctl(epoll_fd, EPOLL_CTL_DEL, client_fd, NULL)==-1)
            {
              std::cout << "removing client_fd failed" << std::endl;
            }
            std::cout << "closing fd" << std::endl;
            close(client_fd);
          }
          else if(res == 0)
          {

          }
        }
      }
    }
    std::cout << "---------------------" << std::endl;
  }
  
  

  return 0;
}

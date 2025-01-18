#include <iostream>
#include<sstream>
#include<optional>
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

#include "file_utils.h"




Argument read_argument(int argc, char **argv) 
{
  // Command line arguments will be stored in a map
  std::unordered_map<std::string, std::string> args;

  // Parse the command line arguments
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg.substr(0, 2) == "--" && i + 1 < argc) {
      args[arg.substr(2)] = argv[++i];
    }
  }

  Argument argument;
  // Check if the port argument was provided
  if (auto it = args.find("port"); it != args.end()) {
    int port = std::stoi(it->second);
    argument.port = port;
    std::cout << "port provided = " << port << std::endl;
  }

  // Check if replicaof argument was provided
  if (auto it = args.find("replicaof"); it != args.end()) {
    std::istringstream iss(it->second);
    std::string host;
    int64_t port;
    iss >> host;
    iss >> port;
    std::cout << "replicaof provided, host = " << host << " , port = " << port << std::endl;
    argument.replicaof_host = std::move(host);
    argument.replicaof_port = port;
  }

  // Check if rdb info dir argument was provided
  if (auto it = args.find("dir"); it != args.end()) {
    argument.dir = it->second;
  }

  // Check if rdb info dbfilename argument was provided
  if (auto it = args.find("dbfilename"); it != args.end()) {
    argument.dbfilename = it->second;
  }

  return argument;
}

const int MAX{1024};
const int BUFFERSIZE{1024};

AllRedisDBs redis = AllRedisDBs();
Argument global_args;



std::vector<std::string> tokenize(const std::string& str, const std::string& delimiter) 
{
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
          redis.dbs["db_1"].kvstore[key] = val;

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
              redis.dbs["db_1"].kvstore_expiries[key] = expiry_epoch;
            }
            else{
              std::cout << "NOT SETTING PX for this " << std::endl;
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
          if(redis.dbs["db_1"].kvstore.find(key) != redis.dbs["db_1"].kvstore.end())
          {
            if(redis.dbs["db_1"].kvstore_expiries.find(key) != redis.dbs["db_1"].kvstore_expiries.end())
            {
              uint64_t current_epoch = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()
              ).count();
              if(current_epoch  <= redis.dbs["db_1"].kvstore_expiries[key])
              {
                std::string val = redis.dbs["db_1"].kvstore[key];
                res = "$" + std::to_string(val.size()) + "\r\n" + val + "\r\n";
              }
              else
              {
                redis.dbs["db_1"].kvstore_expiries.erase(key);
                redis.dbs["db_1"].kvstore.erase(key);
                res = "$-1\r\n";
              }
            }
            else
            {
              std::string val = redis.dbs["db_1"].kvstore[key];
              res = "$" + std::to_string(val.size()) + "\r\n" + val + "\r\n";
            }
          }
          else
            res = "$-1\r\n";

          std::cout << "res = " << res << std::endl;
          write(client_fd, res.c_str(), res.size());
        }
        else if(compareStrings(strs, "CONFIG"))
        {
          std::cout << "config command received" << std::endl;
          ci++; // goto "get" part of "config get foo"
          std::string resp="";
          std::string getcommand = strs_received[ci];
          if(compareStrings(getcommand, "GET"))
          {
            ci++;//goto the requested parameters
            std::string parameter_name = strs_received[ci];
            if(compareStrings(parameter_name, "dir"))
            {
              if(global_args.dir)
              {
                std::string dirname = *(global_args.dir);
                resp = "*2\r\n$3\r\ndir\r\n$" 
                  + std::to_string(dirname.size())
                  +"\r\n"
                  + dirname 
                  +"\r\n";
              }
              else
              {
                resp = "$-1\r\n";
              }

            }
            else if(compareStrings(parameter_name, "dbfilename"))
            {
              if(global_args.dbfilename)
              {
                std::string dbfilename_copied= *(global_args.dbfilename);
                resp = "*2\r\n$10\r\ndbfilename\r\n$" 
                  + std::to_string(dbfilename_copied.size())
                  +"\r\n"
                  + dbfilename_copied
                  +"\r\n";
              }
              else
              {
                resp = "$-1\r\n";
              }
            }
            else
            {
              std::cout << "some other config requested" << std::endl;
            }
            

          }
          else
          {
            std::cout << "config command received but no GET part in it" << std::endl;
          }

          write(client_fd, resp.c_str(), resp.size());
        }
        else if(compareStrings(strs, "SAVE"))
        {
          std::cout << "save command for saving redis db to file received." << std::endl;

        }
        else if(compareStrings(strs, "KEYS"))
        {
          std::cout << "keys command for reading keys from redis db to file received." << std::endl;
          ci++; //goto "*"
          std::string star = strs_received[ci];
          std::vector<std::string> all_keys;
          
          std::cout << "redis.dbs['db_1'].kvstore.size() = " << redis.dbs["db_1"].kvstore.size() << std::endl;
          for(auto pp : redis.dbs["db_1"].kvstore)
          {
            all_keys.push_back(pp.first);
            std::cout << "found key = " << pp.first << std::endl;
          }

          std::string resp = "*";
          resp += std::to_string(all_keys.size());
          resp += "\r\n";
          for(auto kk : all_keys)
          {
            resp += "$";
            resp += std::to_string(kk.size());
            resp += "\r\n";
            resp += kk;
            resp += "\r\n";
          }
          write(client_fd, resp.c_str(), resp.size());
        }
        else if(compareStrings(strs, "INFO"))
        {
          ci++;// goto "replcation", will be ignored for now
          std::string info_arg = strs_received[ci];

          std::string role_string = "role:" + redis.dbs["db_1"].db_role;
          std::string master_repl_offset_string = "master_repl_offset:" + redis.dbs["db_1"].db_master_repl_offset;
          std::string master_replid_string = "master_replid:" + redis.dbs["db_1"].db_master_replid;

          std::string resp_raw = "";
          resp_raw += role_string;
          resp_raw += "\r\n";
          // add master repl offset
          resp_raw += master_repl_offset_string;
          resp_raw += "\r\n";
          //add master_repliid
          resp_raw += master_replid_string;

          std::string resp = "$";
          resp += std::to_string(resp_raw.size());
          resp += "\r\n";
          resp += resp_raw;
          resp += "\r\n";

          std::cout << "sendidng to client = " << resp << std::endl;

          write(client_fd, resp.c_str(), resp.size());

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


  global_args = read_argument(argc, argv);

  redis.dbs["db_1"] = RedisDB();
  auto parsed_db = load_and_parse_rdb(global_args);
  if(parsed_db)
  {
    redis.dbs["db_1"] = *parsed_db;
  }
  else
  {
    std::cout << "Parsed nothing" << std::endl;
  }

  if(global_args.replicaof_host)
  {
    std::cout << "setting role to SLAVE" << std::endl;
    redis.dbs["db_1"].db_role = "slave";
  }
  else
  {
    std::cout << "setting role to MASTER" << std::endl;
    redis.dbs["db_1"].db_role = "master";
  }
  redis.dbs["db_1"].db_master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
  redis.dbs["db_1"].db_master_repl_offset = "0";

  
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
  
  int port_provided = global_args.port ? global_args.port : 6379;
  struct sockaddr_in server_addr;
  server_addr.sin_family = AF_INET;
  server_addr.sin_addr.s_addr = INADDR_ANY;
  server_addr.sin_port = htons(port_provided);
  
  if (bind(server_fd, (struct sockaddr *) &server_addr, sizeof(server_addr)) != 0) {
    std::cerr << "Failed to bind to port " << std::to_string(port_provided) << " \n";
    return 1;
  }
  
  int connection_backlog = 5;
  if (listen(server_fd, connection_backlog) != 0) {
    std::cerr << "listen failed\n";
    return 1;
  }

  // create event loop
  int epoll_fd = epoll_create1(0);
  if(epoll_fd < 0)
  {
    std::cerr << "epoll creation fail" << std::endl;
    return -1;
  }

  struct epoll_event ev;

  // put stdin in event loop
  std::cout << "putting in stdin in epoll " << std::endl;
  ev.events = EPOLLIN;
  ev.data.fd = STDIN_FILENO;
  if(epoll_ctl(epoll_fd, EPOLL_CTL_ADD, STDIN_FILENO, &ev) < 0)
  {
    std::cerr << "error while adding stdin to epoll" << std::endl;
    return -1;
  }

  // put listening server_fd  in event loop
  std::cout << "putting in server_fd in epooll" << std::endl;
  ev.events = EPOLLIN;
  ev.data.fd = server_fd;
  if(epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_fd, &ev) < 0)
  {
    std::cerr << "error while adding server_fd to epoll" << std::endl;
    return -1;
  }

  // if in slave role, connect to replicaof-host and put its fd in epoll
  int slave_sock_fd {-10};
  if(global_args.replicaof_host)
  {
    std::cout << "connect to replicaof-host (the master) , perform handshake, and put its fd in event loop" << std::endl;
    slave_sock_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (setsockopt(slave_sock_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
      std::cerr << "setsockopt for slave_sock_fd failed\n";
      return 1;
    }

    if(slave_sock_fd == -1)
    {
      std::cerr << "slave socket creation failed" << std::endl;
      return  -1;
    }
    else{
      std::cout << "slave socket created" << std::endl;
    }
    
    struct sockaddr_in master_socket_address;
    bzero(&master_socket_address, sizeof(master_socket_address));

    // assign master ip port
    master_socket_address.sin_family = AF_INET;
    master_socket_address.sin_addr.s_addr = inet_addr("127.0.0.1");
    master_socket_address.sin_port = htons(*global_args.replicaof_port);

    // connect the client socket to server socket
    if(connect(slave_sock_fd, (struct sockaddr *)&master_socket_address, sizeof(master_socket_address)) != 0)
    {
      std::cerr << "connection from slave client socket to master server socket failed" << std::endl;
      return -2;
    }
    else
      std::cout << "connected slave socket to master sokcet" << std::endl;

    // put slave_sock_fd in event loop
    ev.events = EPOLLIN;
    ev.data.fd = slave_sock_fd;
    if(epoll_ctl(epoll_fd, EPOLL_CTL_ADD, slave_sock_fd, &ev) < 0)
    {
      std::cerr << "putting slave_sock_fd in event loop failed" << std::endl;
      return -3;
    }
    else
      std::cout << "put slave_sock_fd in event loop success fully" << std::endl;

    // attempting handshake with master_socket

    // part 1 of handshaek : PING
    std::string to_send_to_master = "*1\r\n$4\r\nPING\r\n";
    write(slave_sock_fd, to_send_to_master.c_str(), to_send_to_master.size());

    // part 2 of handshake : after receiving a response to the PING in stage 1, replica sends REPLCONF twice to the master

    // part 3 of handshake : replica sends PSYNC to the master

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
            if(global_args.replicaof_host)
              close(slave_sock_fd);
            return 10;
          }
        }
        else if(epv.data.fd == slave_sock_fd)
        {
          std::cout << "slave_sock_fd received something from somenoe need to fingure out" << std::endl;
        }
        else if(epv.data.fd == server_fd)
        {
          std::cout << "server received a connect request" << std::endl;
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

          // put client_fd in event loop
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

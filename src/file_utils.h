#include<filesystem>
#include<bitset>
#include<fstream>
#include<vector>
#include<tuple>

struct Argument 
{
  int16_t port{kDefaultPort};

  std::optional<std::string> replicaof_host;
  std::optional<int16_t> replicaof_port;

  static constexpr int16_t kDefaultPort = 6379;

  std::optional<std::string> dir;
  std::optional<std::string> dbfilename;

  /*
  ServerConfig to_server_config() {
    ServerConfig config;
    config.port = port;
    if (replicaof_port.has_value()) {
      config.replicaof = {replicaof_host.value(), replicaof_port.value()};
    }
    if (dir.has_value() && dbfilename.has_value()) {
      config.rdb_info = {dir.value(), dbfilename.value()};
    }
    return config;
  }
  */
};

struct KeyValue
{
  uint64_t expiry_time = 0; // Expiry time (in seconds or ms), optional
  uint8_t value_type;                  // Value type flag
  std::string key;                     // Key string
  std::string value;                   // Value string
  uint32_t next_location;
};
struct RedisDB
{
    std::string db_name;
    std::string db_role;
  std::unordered_map<std::string, std::string> kvstore;
  std::unordered_map<std::string, uint64_t> kvstore_expiries;
};

struct AllRedisDBs
{
    std::unordered_map<std::string, RedisDB> dbs; // Map of db_name to RedisData

};

bool directory_exists(const std::string &dir)
{
  return std::filesystem::exists(dir) && std::filesystem::is_directory(dir);
}
// Function to check if a file exists in the given directory
bool file_exists_in_directory(const std::string &dir, const std::string &filename)
{
  std::filesystem::path file_path = std::filesystem::path(dir) / filename;
  return std::filesystem::exists(file_path) && std::filesystem::is_regular_file(file_path);
}

std::vector<char> load_binary_content(const std::string &filename)
{
  std::ifstream file(filename, std::ios::binary);
  if (!file)
  {
    throw std::runtime_error("Failed to open file: " + filename);
  }
  // Move to the end of the file to get the size
  file.seekg(0, std::ios::end);
  size_t file_size = file.tellg();
  file.seekg(0, std::ios::beg);
  // Read the entire file into a vector of chars
  std::vector<char> content(file_size);
  file.read(content.data(), file_size);
  if (!file)
  {
    throw std::runtime_error("Failed to read data from file: " + filename);
  }
  std::cout << "Loaded " << file_size << " bytes from " << filename << std::endl;
  return content;
}

std::pair<uint32_t, uint32_t> parse_length(std::vector<char> &binary_content, uint32_t &memory_location)
{
  uint8_t first_byte = static_cast<uint8_t>(binary_content[memory_location]);
  memory_location++;
  std::bitset<8> first_byte_bin(first_byte); // Creates a bitset for 8 bits (1 byte)
  std::cout << "First byte (binary): " << first_byte_bin << std::endl;
  uint8_t encoding = (first_byte >> 6);
  std::cout << "Encoding (upper 2 bits): " << std::bitset<2>(encoding) << std::endl;
  uint32_t length;
  switch (encoding)
  {
  case 0b00: // Length is in the lower 6 bits
    length = first_byte & 0x3F;
    break;
  case 0b01: // Length is in the next byte (14 bits total)
    length = ((first_byte & 0x3F) << 8) | static_cast<uint8_t>(binary_content[memory_location]);
    memory_location++;
    break;
  case 0b10: // Length is stored in the next 4 bytes
    length = *reinterpret_cast<const uint32_t *>(&binary_content[memory_location]);
    memory_location += 4;
    break;
  case 0b11: // Handle the remaining 6 bits
  {
    uint8_t remaining = first_byte & 0x3F; // Get the remaining 6 bits
    // memory_location++;
    switch (remaining)
    {
    case 0:
      length = 1; // 8-bit integer follows (1 byte)
      break;
    case 1:
      length = 2; // 16-bit integer follows (2 bytes)
      break;
    case 2:
      length = 4; // 32-bit integer follows (4 bytes)
      break;
    default:
      throw std::runtime_error("Unsupported length encoding value in 0b11 case.");
    }
    break;
  }
  default:
    throw std::runtime_error("Unsupported or special length encoding.");
  }
  return std::pair(length, memory_location);
};


std::tuple<uint32_t, uint32_t, uint32_t> parse_resize_db_field(std::vector<char> &binary_content, uint32_t memory_location)
{
  uint8_t opcode = static_cast<uint8_t>(binary_content[memory_location]);
  if (opcode != 0xFB)
  {
    throw std::runtime_error("Unexpected opcode found. Expected AUX (0xFB).");
  }
  memory_location++;
  // return a pair where the first = the database number and the second = the new memory location.
  // Perhaps we have to add one..
  auto result_hash_table_size = parse_length(binary_content, memory_location);
  auto result_hash_table_expire_size = parse_length(binary_content, result_hash_table_size.second);
  // memory_location++; ?Maybe?
    // Hash table size, hash table expire size, new memory location
  return std::tuple(result_hash_table_size.first, result_hash_table_expire_size.first, result_hash_table_expire_size.second);
}

// Function to derive the database name from its number
std::string derive_db_name(uint8_t db_num)
{
  return "db_" + std::to_string(db_num);
}

std::pair<std::pair<std::string, std::string>, uint32_t> parse_aux(std::vector<char> &binary_content, uint32_t memory_location)
{
  // Ensure we're looking at an AUX opcode (0xFA)
  uint8_t opcode = static_cast<uint8_t>(binary_content[memory_location]);
  if (opcode != 0xFA)
  {
    throw std::runtime_error("Unexpected opcode found. Expected AUX (0xFA).");
  }
  // Move past the opcode
  memory_location++;
  // Parse the key length
  std::cout << "Parsing Length Key.." << std::endl;
  auto key_result = parse_length(binary_content, memory_location);
  uint32_t key_length = key_result.first;
  memory_location = key_result.second;
  // Parse the key string
  if (memory_location + key_length > binary_content.size())
  {
    throw std::runtime_error("Key length exceeds available binary content.");
  }
  std::string key(binary_content.begin() + memory_location, binary_content.begin() + memory_location + key_length);
  memory_location += key_length;
  // Parse the value length
  std::cout << "Parsing Length Value.. Key: " << key << ". Len: " << std::to_string(key_length) << std::endl;
  auto result_result = parse_length(binary_content, memory_location);
  uint32_t value_length = result_result.first;
  memory_location = result_result.second;
  std::cout << "Value Length: " << std::to_string(value_length) << std::endl;
  // Parse the value string
  if (memory_location + value_length > binary_content.size())
  {
    throw std::runtime_error("Value length exceeds available binary content.");
  }
  std::string value(binary_content.begin() + memory_location, binary_content.begin() + memory_location + value_length);
  memory_location += value_length;
  // Return the key-value pair along with the updated memory location
  return {{key, value}, memory_location};
}

KeyValue parseKeyValue(std::vector<char> &binary_content, uint32_t &memory_location)
{
  KeyValue result;
  // Read the first byte to determine if there is an expiry time
  uint8_t first_byte = static_cast<uint8_t>(binary_content[memory_location]);
  // Parse expiry time (if present)
  if (first_byte == 0xFD)
  { // Expiry in seconds
    memory_location++;
    if (memory_location + 4 > binary_content.size())
    {
      throw std::runtime_error("Unexpected EOF while parsing expiry time (seconds).");
    }
    result.expiry_time = *reinterpret_cast<const uint32_t *>(&binary_content[memory_location]);
    memory_location += 4;
  }
  else if (first_byte == 0xFC)
  { // Expiry in milliseconds
    memory_location++;
    if (memory_location + 8 > binary_content.size())
    {
      throw std::runtime_error("Unexpected EOF while parsing expiry time (milliseconds).");
    }
    result.expiry_time = *reinterpret_cast<const uint64_t *>(&binary_content[memory_location]);
    memory_location += 8;
  }
  // Parse the value type
  if (memory_location >= binary_content.size())
  {
    throw std::runtime_error("Unexpected EOF while parsing value type.");
  }
  result.value_type = static_cast<uint8_t>(binary_content[memory_location]);
  if (result.value_type != 0)
  {
    throw std::runtime_error("Only string encoding supported for now. Found " + std::to_string(result.value_type));
  }
  memory_location++;
  // Parse the key
  auto [size_of_key, new_memory_start] = parse_length(binary_content, memory_location);
  result.key = std::string(binary_content.begin() + new_memory_start, binary_content.begin() + new_memory_start + size_of_key);
  new_memory_start += size_of_key;
  // Parse the value
  auto [size_of_value, value_memory_start] = parse_length(binary_content, new_memory_start);
  result.value = std::string(binary_content.begin() + value_memory_start, binary_content.begin() + value_memory_start + size_of_value);
  value_memory_start += size_of_value;
  result.next_location = value_memory_start;
  return result;
}

uint32_t parse_single_db(std::vector<char> &binary_content, uint32_t memory_location, RedisDB &redis_data)
{
  std::cout << "Starting to parse single database at memory location: " << memory_location << std::endl;
  uint8_t db_number = 0; // Declare db_number outside the switch to avoid crossing initialization
  bool encountered_fe_once = false;
  while (memory_location < binary_content.size())
  {
    uint8_t opcode = static_cast<uint8_t>(binary_content[memory_location]);
    std::cout << "Parsing opcode: " << std::hex << static_cast<int>(opcode) << std::dec << " at location: " << memory_location << std::endl;
    switch (opcode)
    {
    case 0xFE: // Database name..
      std::cout << "Found database switch marker (0xFE). Ending this DB parsing." << std::endl;
      memory_location++;
    //   db_number = static_cast<uint8_t>(memory_location); // Initialize here
      db_number = static_cast<uint8_t>(1); // Initialize here
      redis_data.db_name = derive_db_name(db_number);
      memory_location++;
      if (encountered_fe_once)
      {
        return memory_location;
      }
      encountered_fe_once = true;
      break; // Return the current memory location to indicate the end of this DB
    case 0xFB:
    { // Resize DB hash table
      std::cout << "Found resize DB hash table marker (0xFB)." << std::endl;
      auto [size, expire_size, new_memory_location] = parse_resize_db_field(binary_content, memory_location);
      memory_location = new_memory_location;
      std::cout << "Resize DB field parsed. New memory location: " << memory_location << std::endl;
      break;
    }
    case 0xFA:
    { // AUX field
      std::cout << "Found AUX field marker (0xFA)." << std::endl;
      auto [aux_pair, new_memory_location] = parse_aux(binary_content, memory_location);
      memory_location = new_memory_location;
      std::cout << "AUX field parsed. New memory location: " << memory_location << std::endl;
      break;
    }
    case 0xFF:
    { // End of RDB file
      std::cout << "Found end of RDB file marker (0xFF). Skipping 9 bytes." << std::endl;
      memory_location += 9;   // Skip the 0xFF opcode and the checksum (8 bytes)
      return memory_location; // End of parsing
    }
    default:
    { // Key-value pair (with or without expiry)
      try
      {
        std::cout << "Parsing key-value pair at location: " << memory_location << std::endl;
        KeyValue kv = parseKeyValue(binary_content, memory_location);
        redis_data.kvstore[kv.key] = kv.value;
        redis_data.kvstore_expiries[kv.key] = kv.expiry_time ? kv.expiry_time : -1;
        memory_location = kv.next_location;
        std::cout << "Parsed key: " << kv.key << ", Value: " << kv.value << ", Next location: " << memory_location << std::endl;
      }
      catch (const std::runtime_error &e)
      {
        std::cerr << "Error parsing key-value pair: " << e.what() << std::endl;
        throw std::runtime_error("Error parsing key-value pair: " + std::string(e.what()));
      }
      break;
    }
    }
  }
  std::cerr << "Unexpected EOF without a database switch or file end." << std::endl;
  throw std::runtime_error("Unexpected EOF without a database switch or file end.");
}


AllRedisDBs parse_all_databases(std::vector<char> &binary_content)
{
  AllRedisDBs all_data;
  uint32_t memory_location = 9; // Skip the "REDIS" magic string and version
  std::cout << "Starting to parse all databases from memory location: " << memory_location << std::endl;
  uint8_t opcode = static_cast<uint8_t>(binary_content[memory_location]);
  RedisDB redis_data;
  memory_location = parse_single_db(binary_content, memory_location, redis_data);
  std::cout << "Parsed database " << redis_data.db_name << " and added to all_data." << std::endl;
  all_data.dbs = {{redis_data.db_name, redis_data}};
  return all_data;
}


void log_parsed_data(AllRedisDBs &all_data)
{
  std::cout << "Parsing complete. Parsed " << all_data.dbs.size() << " databases." << std::endl;
  for (auto &db_entry : all_data.dbs)
  {
    std::cout << "Database: " << db_entry.first << std::endl;
    RedisDB &db = db_entry.second;
    std::cout << "  Key-Value pairs:" << std::endl;
    for (auto kv_entry : db.kvstore)
    {
        std::string key = kv_entry.first;
        std::string value = kv_entry.second;
        uint64_t expiry_time = (db.kvstore_expiries.find(key) != db.kvstore_expiries.end()) ? db.kvstore_expiries[key] : 0;
      std::cout << "    Key: " << key << ", Value: " << value
              << ", Expiry: " << (expiry_time ? std::to_string(expiry_time) : "None") << std::endl;
    }
  }
}

void assert_single_db(const AllRedisDBs &all_data)
{
  if (all_data.dbs.size() != 1)
  {
    throw std::runtime_error("Error: More than one database found.");
  }
}

std::optional<RedisDB> load_and_parse_rdb(const Argument &args)
{
  try
  {
    // Initialize configuration
    // initializeConf(args);
    // Check if dir and dbfilename are set in the configuration
    std::cout << "reading args from Argument structure optionasl" << std::endl;
    if (args.dir && args.dbfilename)
    {
      std::string dir = *(args.dir);
      std::string dbfilename = *(args.dbfilename);
      std::string file_path = dir + "/" + dbfilename;
      // Check if the directory exists
      if (directory_exists(dir))
      {
        std::cout << "Directory found: " << dir << std::endl;
        // Check if the file exists in the directory
        if (file_exists_in_directory(dir, dbfilename))
        {
          std::cout << "Loading RDB file from: " << file_path << std::endl;
          std::vector<char> binary_content = load_binary_content(file_path);
          // Parse all databases
          std::cout << "Starting to parse RDB file..." << std::endl;
          AllRedisDBs all_data = parse_all_databases(binary_content);
          log_parsed_data(all_data);
          assert_single_db(all_data);
          // Extract the single RedisData from the RedisDataAll
          RedisDB &single_db_data = all_data.dbs.begin()->second;
          // Convert the database to the in-memory representation
        //   convert_db_to_memory_map(single_db_data);
          for (auto &entry :single_db_data.kvstore )
          {
            const std::string &key = entry.first;
            const std::string &value = entry.second;
            uint64_t expiry_time = (single_db_data.kvstore_expiries.find(key) != single_db_data.kvstore_expiries.end()) ? single_db_data.kvstore_expiries[key] : 0;
            // const std::tuple<std::string, std::optional<int>> &value_and_expiry = entry.second;
            // const std::string &value = std::get<0>(value_and_expiry);
            // const std::optional<int> &expiry = std::get<1>(value_and_expiry);
            std::cout << "Key: " << key << ", Value: " << value;
            if (expiry_time)
            {
              std::cout << ", Expiry: " << expiry_time;
            }
            else
            {
              std::cout << ", Expiry: None";
            }
            std::cout << std::endl;
          }
          std::cout << "RDB file parsing completed successfully." << std::endl;
          std::cout << "setting inmemory to the read from file." << std::endl;
          return single_db_data;
        }
        else
        {
          std::cout << "File does not exist. Treating as empty database." << std::endl;
          // Proceed with empty database (handle accordingly)
          AllRedisDBs empty_data; // An empty RedisDataAll structure
          log_parsed_data(empty_data);
          
        }
      }
      else
      {
        std::cerr << "Directory does not exist: " << dir << std::endl;
         // Exit with error if the directory doesn't exist
      }
    }
    else
    {
      std::cerr << "Error: dir or dbfilename not set in configuration." << std::endl;
       // Exit with error if required configurations are missing
    }
  }
  catch (const std::runtime_error &e)
  {
    std::cerr << "Error: " << e.what() << std::endl;
  }
  return {};
}
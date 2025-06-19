#include "query5.hpp"
#include <iostream>
#include <fstream>
#include <sstream>
#include <thread>
#include <mutex>
#include <algorithm>
#include <unordered_map>
#include <unordered_set>

// Global mutex for thread-safe result aggregation
std::mutex result_mutex;

// Function to parse command line arguments
bool parseArgs(int argc, char* argv[], std::string& r_name, std::string& start_date, std::string& end_date, int& num_threads, std::string& table_path, std::string& result_path) {
    // Set default values
    r_name = "ASIA";
    start_date = "1994-01-01";
    end_date = "1995-01-01";
    num_threads = 1;
    table_path = "./";
    result_path = "./results.txt";
    
    for (int i = 1; i < argc; i += 2) {
        if (i + 1 >= argc) break; // Ensure we have a value for each argument
        
        std::string arg = argv[i];
        std::string value = argv[i + 1];
        
        if (arg == "--r_name") {
            r_name = value;
        } else if (arg == "--start_date") {
            start_date = value;
        } else if (arg == "--end_date") {
            end_date = value;
        } else if (arg == "--threads") {
            num_threads = std::stoi(value);
        } else if (arg == "--table_path") {
            table_path = value;
            // Ensure path ends with /
            if (table_path.back() != '/') {
                table_path += "/";
            }
        } else if (arg == "--result_path") {
            result_path = value;
        }
    }
    
    return true;
}

// Helper function to split string by delimiter
std::vector<std::string> split(const std::string& str, char delimiter) {
    std::vector<std::string> tokens;
    std::stringstream ss(str);
    std::string token;
    
    while (std::getline(ss, token, delimiter)) {
        tokens.push_back(token);
    }
    
    return tokens;
}

// Function to read TPCH data from the specified paths
bool readTPCHData(const std::string& table_path, 
                  std::vector<std::map<std::string, std::string>>& customer_data,
                  std::vector<std::map<std::string, std::string>>& orders_data,
                  std::vector<std::map<std::string, std::string>>& lineitem_data,
                  std::vector<std::map<std::string, std::string>>& supplier_data,
                  std::vector<std::map<std::string, std::string>>& nation_data,
                  std::vector<std::map<std::string, std::string>>& region_data) {
    
    // Read customer.tbl
    std::ifstream customer_file(table_path + "customer.tbl");
    if (!customer_file.is_open()) {
        std::cerr << "Error: Cannot open customer.tbl" << std::endl;
        return false;
    }
    
    std::string line;
    while (std::getline(customer_file, line)) {
        std::vector<std::string> tokens = split(line, '|');
        if (tokens.size() >= 4) {
            std::map<std::string, std::string> row;
            row["c_custkey"] = tokens[0];
            row["c_name"] = tokens[1];
            row["c_address"] = tokens[2];
            row["c_nationkey"] = tokens[3];
            customer_data.push_back(row);
        }
    }
    customer_file.close();
    
    // Read orders.tbl
    std::ifstream orders_file(table_path + "orders.tbl");
    if (!orders_file.is_open()) {
        std::cerr << "Error: Cannot open orders.tbl" << std::endl;
        return false;
    }
    
    while (std::getline(orders_file, line)) {
        std::vector<std::string> tokens = split(line, '|');
        if (tokens.size() >= 5) {
            std::map<std::string, std::string> row;
            row["o_orderkey"] = tokens[0];
            row["o_custkey"] = tokens[1];
            row["o_orderstatus"] = tokens[2];
            row["o_totalprice"] = tokens[3];
            row["o_orderdate"] = tokens[4];
            orders_data.push_back(row);
        }
    }
    orders_file.close();
    
    // Read lineitem.tbl
    std::ifstream lineitem_file(table_path + "lineitem.tbl");
    if (!lineitem_file.is_open()) {
        std::cerr << "Error: Cannot open lineitem.tbl" << std::endl;
        return false;
    }
    
    while (std::getline(lineitem_file, line)) {
        std::vector<std::string> tokens = split(line, '|');
        if (tokens.size() >= 7) {
            std::map<std::string, std::string> row;
            row["l_orderkey"] = tokens[0];
            row["l_partkey"] = tokens[1];
            row["l_suppkey"] = tokens[2];
            row["l_linenumber"] = tokens[3];
            row["l_quantity"] = tokens[4];
            row["l_extendedprice"] = tokens[5];
            row["l_discount"] = tokens[6];
            lineitem_data.push_back(row);
        }
    }
    lineitem_file.close();
    
    // Read supplier.tbl
    std::ifstream supplier_file(table_path + "supplier.tbl");
    if (!supplier_file.is_open()) {
        std::cerr << "Error: Cannot open supplier.tbl" << std::endl;
        return false;
    }
    
    while (std::getline(supplier_file, line)) {
        std::vector<std::string> tokens = split(line, '|');
        if (tokens.size() >= 4) {
            std::map<std::string, std::string> row;
            row["s_suppkey"] = tokens[0];
            row["s_name"] = tokens[1];
            row["s_address"] = tokens[2];
            row["s_nationkey"] = tokens[3];
            supplier_data.push_back(row);
        }
    }
    supplier_file.close();
    
    // Read nation.tbl
    std::ifstream nation_file(table_path + "nation.tbl");
    if (!nation_file.is_open()) {
        std::cerr << "Error: Cannot open nation.tbl" << std::endl;
        return false;
    }
    
    while (std::getline(nation_file, line)) {
        std::vector<std::string> tokens = split(line, '|');
        if (tokens.size() >= 3) {
            std::map<std::string, std::string> row;
            row["n_nationkey"] = tokens[0];
            row["n_name"] = tokens[1];
            row["n_regionkey"] = tokens[2];
            nation_data.push_back(row);
        }
    }
    nation_file.close();
    
    // Read region.tbl
    std::ifstream region_file(table_path + "region.tbl");
    if (!region_file.is_open()) {
        std::cerr << "Error: Cannot open region.tbl" << std::endl;
        return false;
    }
    
    while (std::getline(region_file, line)) {
        std::vector<std::string> tokens = split(line, '|');
        if (tokens.size() >= 2) {
            std::map<std::string, std::string> row;
            row["r_regionkey"] = tokens[0];
            row["r_name"] = tokens[1];
            region_data.push_back(row);
        }
    }
    region_file.close();
    
    return true;
}

// Worker function for multithreading
void processOrdersChunk(const std::vector<std::map<std::string, std::string>>& orders_chunk,
                       const std::string& start_date,
                       const std::string& end_date,
                       const std::vector<std::map<std::string, std::string>>& customer_data,
                       const std::vector<std::map<std::string, std::string>>& lineitem_data,
                       const std::vector<std::map<std::string, std::string>>& supplier_data,
                       const std::unordered_map<std::string, std::string>& nation_names,
                       const std::unordered_set<std::string>& target_nations,
                       std::map<std::string, double>& global_results) {
    
    std::map<std::string, double> local_results;
    
    // Build lookup maps for this thread
    std::unordered_map<std::string, std::string> customer_nation_map;
    std::unordered_map<std::string, std::string> supplier_nation_map;
    
    for (const auto& customer : customer_data) {
        customer_nation_map[customer.at("c_custkey")] = customer.at("c_nationkey");
    }
    
    for (const auto& supplier : supplier_data) {
        supplier_nation_map[supplier.at("s_suppkey")] = supplier.at("s_nationkey");
    }
    
    // Process orders chunk
    for (const auto& order : orders_chunk) {
        const std::string& order_date = order.at("o_orderdate");
        
        // Filter by date
        if (order_date >= start_date && order_date < end_date) {
            const std::string& custkey = order.at("o_custkey");
            const std::string& orderkey = order.at("o_orderkey");
            
            auto cust_nation_it = customer_nation_map.find(custkey);
            if (cust_nation_it != customer_nation_map.end()) {
                const std::string& cust_nationkey = cust_nation_it->second;
                
                // Check if customer nation is in target region
                if (target_nations.count(cust_nationkey)) {
                    // Find matching lineitems
                    for (const auto& lineitem : lineitem_data) {
                        if (lineitem.at("l_orderkey") == orderkey) {
                            const std::string& suppkey = lineitem.at("l_suppkey");
                            
                            auto supp_nation_it = supplier_nation_map.find(suppkey);
                            if (supp_nation_it != supplier_nation_map.end()) {
                                const std::string& supp_nationkey = supp_nation_it->second;
                                
                                // Check if customer and supplier are from same nation
                                if (cust_nationkey == supp_nationkey) {
                                    double extendedprice = std::stod(lineitem.at("l_extendedprice"));
                                    double discount = std::stod(lineitem.at("l_discount"));
                                    double revenue = extendedprice * (1.0 - discount);
                                    
                                    std::string nation_name = nation_names.at(cust_nationkey);
                                    local_results[nation_name] += revenue;
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    
    // Merge local results with global results (thread-safe)
    std::lock_guard<std::mutex> lock(result_mutex);
    for (const auto& pair : local_results) {
        global_results[pair.first] += pair.second;
    }
}

// Function to execute TPCH Query 5 using multithreading
bool executeQuery5(const std::string& r_name,
                  const std::string& start_date,
                  const std::string& end_date,
                  int num_threads,
                  const std::vector<std::map<std::string, std::string>>& customer_data,
                  const std::vector<std::map<std::string, std::string>>& orders_data,
                  const std::vector<std::map<std::string, std::string>>& lineitem_data,
                  const std::vector<std::map<std::string, std::string>>& supplier_data,
                  const std::vector<std::map<std::string, std::string>>& nation_data,
                  const std::vector<std::map<std::string, std::string>>& region_data,
                  std::map<std::string, double>& results) {
    
    results.clear();
    
    // Build lookup maps
    std::unordered_map<std::string, std::string> nation_names; // nationkey -> nation_name
    std::unordered_set<std::string> target_nations; // nationkeys in target region
    
    for (const auto& nation : nation_data) {
        nation_names[nation.at("n_nationkey")] = nation.at("n_name");
    }
    
    // Find nations in target region
    for (const auto& region : region_data) {
        if (region.at("r_name") == r_name) {
            const std::string& target_regionkey = region.at("r_regionkey");
            
            for (const auto& nation : nation_data) {
                if (nation.at("n_regionkey") == target_regionkey) {
                    target_nations.insert(nation.at("n_nationkey"));
                }
            }
            break;
        }
    }
    
    if (target_nations.empty()) {
        std::cerr << "Error: No nations found for region " << r_name << std::endl;
        return false;
    }
    
    if (num_threads == 1) {
        // Single-threaded execution
        std::vector<std::map<std::string, std::string>> orders_chunk(orders_data);
        processOrdersChunk(orders_chunk, start_date, end_date, customer_data, lineitem_data,
                          supplier_data, nation_names, target_nations, results);
    } else {
        // Multi-threaded execution
        std::vector<std::thread> threads;
        int chunk_size = orders_data.size() / num_threads;
        
        for (int i = 0; i < num_threads; ++i) {
            int start_idx = i * chunk_size;
            int end_idx = (i == num_threads - 1) ? orders_data.size() : (i + 1) * chunk_size;
            
            std::vector<std::map<std::string, std::string>> orders_chunk(
                orders_data.begin() + start_idx, orders_data.begin() + end_idx);
            
            threads.emplace_back(processOrdersChunk, orders_chunk, start_date, end_date,
                               std::ref(customer_data), std::ref(lineitem_data), std::ref(supplier_data),
                               std::ref(nation_names), std::ref(target_nations), std::ref(results));
        }
        
        // Wait for all threads to complete
        for (auto& thread : threads) {
            thread.join();
        }
    }
    
    return true;
}

// Function to output results to the specified path
bool outputResults(const std::string& result_path, const std::map<std::string, double>& results) {
    std::ofstream output_file(result_path);
    if (!output_file.is_open()) {
        std::cerr << "Error: Cannot open output file " << result_path << std::endl;
        return false;
    }
    
    // Convert map to vector for sorting
    std::vector<std::pair<std::string, double>> sorted_results(results.begin(), results.end());
    
    // Sort by revenue in descending order
    std::sort(sorted_results.begin(), sorted_results.end(),
              [](const std::pair<std::string, double>& a, const std::pair<std::string, double>& b) {
                  return a.second > b.second;
              });
    
    // Write to file and console
    std::cout << "\nQuery Results:" << std::endl;
    std::cout << "Nation\t\tRevenue" << std::endl;
    std::cout << "------\t\t-------" << std::endl;
    
    output_file << "Nation\tRevenue" << std::endl;
    for (const auto& result : sorted_results) {
        std::cout << result.first << "\t\t" << std::fixed << std::setprecision(2) << result.second << std::endl;
        output_file << result.first << "\t" << result.second << std::endl;
    }
    
    output_file.close();
    return true;
}

#include "query5.hpp"
#include <iostream>
#include <fstream>
#include <sstream>
#include <thread>
#include <mutex>
#include <algorithm>
#include <vector>
#include <map>
#include <string>
#include <iomanip>

// Mutex for thread-safe access to results
std::mutex results_mutex;

// Function to parse command line arguments
bool parseArgs(int argc, char* argv[], std::string& r_name, std::string& start_date, std::string& end_date, int& num_threads, std::string& table_path, std::string& result_path) {
    for (int i = 1; i < argc; i += 2) {
        if (i + 1 >= argc) {
            std::cerr << "Error: Missing value for argument " << argv[i] << std::endl;
            return false;
        }
        
        std::string arg = argv[i];
        std::string value = argv[i + 1];
        
        if (arg == "--r_name") {
            r_name = value;
        } else if (arg == "--start_date") {
            start_date = value;
        } else if (arg == "--end_date") {
            end_date = value;
        } else if (arg == "--threads") {
            try {
                num_threads = std::stoi(value);
                if (num_threads <= 0) {
                    std::cerr << "Error: Number of threads must be positive" << std::endl;
                    return false;
                }
            } catch (const std::exception& e) {
                std::cerr << "Error: Invalid number of threads: " << value << std::endl;
                return false;
            }
        } else if (arg == "--table_path") {
            table_path = value;
        } else if (arg == "--result_path") {
            result_path = value;
        } else {
            std::cerr << "Error: Unknown argument " << arg << std::endl;
            return false;
        }
    }
    
    // Validate required arguments
    if (r_name.empty() || start_date.empty() || end_date.empty() || 
        table_path.empty() || result_path.empty() || num_threads == 0) {
        std::cerr << "Error: Missing required arguments" << std::endl;
        std::cerr << "Usage: --r_name REGION --start_date YYYY-MM-DD --end_date YYYY-MM-DD --threads N --table_path PATH --result_path PATH" << std::endl;
        return false;
    }
    
    return true;
}

// Helper function to read CSV file
std::vector<std::map<std::string, std::string>> readCSV(const std::string& filename) {
    std::vector<std::map<std::string, std::string>> data;
    std::ifstream file(filename);
    
    if (!file.is_open()) {
        std::cerr << "Error: Cannot open file " << filename << std::endl;
        return data;
    }
    
    std::string line;
    std::vector<std::string> headers;
    
    // Read header line
    if (std::getline(file, line)) {
        std::stringstream ss(line);
        std::string cell;
        
        while (std::getline(ss, cell, '|')) {
            headers.push_back(cell);
        }
    }
    
    // Read data lines
    while (std::getline(file, line)) {
        if (line.empty()) continue;
        
        std::stringstream ss(line);
        std::string cell;
        std::map<std::string, std::string> row;
        size_t col_index = 0;
        
        while (std::getline(ss, cell, '|') && col_index < headers.size()) {
            row[headers[col_index]] = cell;
            col_index++;
        }
        
        if (!row.empty()) {
            data.push_back(row);
        }
    }
    
    file.close();
    return data;
}

// Function to read TPCH data from the specified paths
bool readTPCHData(const std::string& table_path, 
                  std::vector<std::map<std::string, std::string>>& customer_data,
                  std::vector<std::map<std::string, std::string>>& orders_data,
                  std::vector<std::map<std::string, std::string>>& lineitem_data,
                  std::vector<std::map<std::string, std::string>>& supplier_data,
                  std::vector<std::map<std::string, std::string>>& nation_data,
                  std::vector<std::map<std::string, std::string>>& region_data) {
    
    std::string base_path = table_path;
    if (base_path.back() != '/') {
        base_path += '/';
    }
    
    try {
        std::cout << "Reading TPC-H data..." << std::endl;
        
        customer_data = readCSV(base_path + "customer.tbl");
        if (customer_data.empty()) {
            std::cerr << "Error: Failed to read customer data" << std::endl;
            return false;
        }
        std::cout << "Read " << customer_data.size() << " customer records" << std::endl;
        
        orders_data = readCSV(base_path + "orders.tbl");
        if (orders_data.empty()) {
            std::cerr << "Error: Failed to read orders data" << std::endl;
            return false;
        }
        std::cout << "Read " << orders_data.size() << " order records" << std::endl;
        
        lineitem_data = readCSV(base_path + "lineitem.tbl");
        if (lineitem_data.empty()) {
            std::cerr << "Error: Failed to read lineitem data" << std::endl;
            return false;
        }
        std::cout << "Read " << lineitem_data.size() << " lineitem records" << std::endl;
        
        supplier_data = readCSV(base_path + "supplier.tbl");
        if (supplier_data.empty()) {
            std::cerr << "Error: Failed to read supplier data" << std::endl;
            return false;
        }
        std::cout << "Read " << supplier_data.size() << " supplier records" << std::endl;
        
        nation_data = readCSV(base_path + "nation.tbl");
        if (nation_data.empty()) {
            std::cerr << "Error: Failed to read nation data" << std::endl;
            return false;
        }
        std::cout << "Read " << nation_data.size() << " nation records" << std::endl;
        
        region_data = readCSV(base_path + "region.tbl");
        if (region_data.empty()) {
            std::cerr << "Error: Failed to read region data" << std::endl;
            return false;
        }
        std::cout << "Read " << region_data.size() << " region records" << std::endl;
        
        return true;
    } catch (const std::exception& e) {
        std::cerr << "Error reading TPC-H data: " << e.what() << std::endl;
        return false;
    }
}

// Worker function for processing a chunk of lineitem data
void processLineitemChunk(const std::vector<std::map<std::string, std::string>>& lineitem_chunk,
                         const std::string& r_name,
                         const std::string& start_date,
                         const std::string& end_date,
                         const std::vector<std::map<std::string, std::string>>& customer_data,
                         const std::vector<std::map<std::string, std::string>>& orders_data,
                         const std::vector<std::map<std::string, std::string>>& supplier_data,
                         const std::vector<std::map<std::string, std::string>>& nation_data,
                         const std::vector<std::map<std::string, std::string>>& region_data,
                         std::map<std::string, double>& local_results) {
    
    // Create lookup maps for faster access
    std::map<std::string, std::map<std::string, std::string>> customer_map;
    for (const auto& customer : customer_data) {
        customer_map[customer.at("C_CUSTKEY")] = customer;
    }
    
    std::map<std::string, std::map<std::string, std::string>> orders_map;
    for (const auto& order : orders_data) {
        const std::string& order_date = order.at("O_ORDERDATE");
        if (order_date >= start_date && order_date < end_date) {
            orders_map[order.at("O_ORDERKEY")] = order;
        }
    }
    
    std::map<std::string, std::map<std::string, std::string>> supplier_map;
    for (const auto& supplier : supplier_data) {
        supplier_map[supplier.at("S_SUPPKEY")] = supplier;
    }
    
    std::map<std::string, std::string> nation_map;
    for (const auto& nation : nation_data) {
        nation_map[nation.at("N_NATIONKEY")] = nation.at("N_NAME");
    }
    
    std::map<std::string, std::string> region_nation_map;
    for (const auto& nation : nation_data) {
        for (const auto& region : region_data) {
            if (nation.at("N_REGIONKEY") == region.at("R_REGIONKEY") && 
                region.at("R_NAME") == r_name) {
                region_nation_map[nation.at("N_NATIONKEY")] = nation.at("N_NAME");
            }
        }
    }
    
    // Process lineitem chunk
    for (const auto& lineitem : lineitem_chunk) {
        const std::string& order_key = lineitem.at("L_ORDERKEY");
        const std::string& supp_key = lineitem.at("L_SUPPKEY");
        
        // Check if order exists in our filtered orders
        auto order_it = orders_map.find(order_key);
        if (order_it == orders_map.end()) continue;
        
        const auto& order = order_it->second;
        const std::string& cust_key = order.at("O_CUSTKEY");
        
        // Check if customer exists
        auto customer_it = customer_map.find(cust_key);
        if (customer_it == customer_map.end()) continue;
        
        const auto& customer = customer_it->second;
        const std::string& cust_nation_key = customer.at("C_NATIONKEY");
        
        // Check if supplier exists
        auto supplier_it = supplier_map.find(supp_key);
        if (supplier_it == supplier_map.end()) continue;
        
        const auto& supplier = supplier_it->second;
        const std::string& supp_nation_key = supplier.at("S_NATIONKEY");
        
        // Check if customer and supplier are from the same nation in our target region
        if (cust_nation_key == supp_nation_key) {
            auto region_nation_it = region_nation_map.find(cust_nation_key);
            if (region_nation_it != region_nation_map.end()) {
                const std::string& nation_name = region_nation_it->second;
                
                // Calculate revenue
                double extended_price = std::stod(lineitem.at("L_EXTENDEDPRICE"));
                double discount = std::stod(lineitem.at("L_DISCOUNT"));
                double revenue = extended_price * (1.0 - discount);
                
                local_results[nation_name] += revenue;
            }
        }
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
    
    try {
        std::cout << "Executing TPC-H Query 5..." << std::endl;
        std::cout << "Region: " << r_name << ", Date range: " << start_date << " to " << end_date << std::endl;
        std::cout << "Using " << num_threads << " threads" << std::endl;
        
        // Calculate chunk size for lineitem data
        size_t total_lineitems = lineitem_data.size();
        size_t chunk_size = (total_lineitems + num_threads - 1) / num_threads;
        
        std::vector<std::thread> threads;
        std::vector<std::map<std::string, double>> thread_results(num_threads);
        
        // Create and start threads
        for (int i = 0; i < num_threads; i++) {
            size_t start_idx = i * chunk_size;
            size_t end_idx = std::min(start_idx + chunk_size, total_lineitems);
            
            if (start_idx >= total_lineitems) break;
            
            std::vector<std::map<std::string, std::string>> chunk(
                lineitem_data.begin() + start_idx,
                lineitem_data.begin() + end_idx
            );
            
            threads.emplace_back(processLineitemChunk,
                               std::ref(chunk),
                               std::ref(r_name),
                               std::ref(start_date),
                               std::ref(end_date),
                               std::ref(customer_data),
                               std::ref(orders_data),
                               std::ref(supplier_data),
                               std::ref(nation_data),
                               std::ref(region_data),
                               std::ref(thread_results[i]));
        }
        
        // Wait for all threads to complete
        for (auto& thread : threads) {
            thread.join();
        }
        
        // Combine results from all threads
        results.clear();
        for (const auto& thread_result : thread_results) {
            for (const auto& pair : thread_result) {
                results[pair.first] += pair.second;
            }
        }
        
        std::cout << "Query execution completed. Found " << results.size() << " nations." << std::endl;
        return true;
        
    } catch (const std::exception& e) {
        std::cerr << "Error executing query: " << e.what() << std::endl;
        return false;
    }
}

// Function to output results to the specified path
bool outputResults(const std::string& result_path, const std::map<std::string, double>& results) {
    try {
        std::ofstream file(result_path);
        if (!file.is_open()) {
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
        
        // Write header
        file << "N_NAME|REVENUE" << std::endl;
        
        // Write results
        for (const auto& pair : sorted_results) {
            file << pair.first << "|" << std::fixed << std::setprecision(2) << pair.second << std::endl;
        }
        
        file.close();
        
        std::cout << "Results written to " << result_path << std::endl;
        
        // Also print to console
        std::cout << "\nTop 10 Results:" << std::endl;
        std::cout << "Nation Name\t\tRevenue" << std::endl;
        std::cout << "----------------------------------------" << std::endl;
        
        int count = 0;
        for (const auto& pair : sorted_results) {
            if (count >= 10) break;
            std::cout << std::left << std::setw(20) << pair.first 
                     << std::fixed << std::setprecision(2) << pair.second << std::endl;
            count++;
        }
        
        return true;
        
    } catch (const std::exception& e) {
        std::cerr << "Error writing results: " << e.what() << std::endl;
        return false;
    }
}

#include "query5.hpp"
#include <iostream>
#include <string>
#include <vector>
#include <thread>
#include <mutex>
#include <fstream>
#include <sstream>
#include <algorithm>
#include <map>
#include <iomanip>          // For std::setprecision
#include <unordered_map>    // For std::unordered_map used in query5.cpp
#include <unordered_set>    // For std::unordered_set used in query5.cpp
#include <chrono>           // For timing measurements (optional but useful)

int main(int argc, char* argv[]) {
    // Optional: Add timing to measure performance
    auto start_time = std::chrono::high_resolution_clock::now();
    
    std::string r_name, start_date, end_date, table_path, result_path;
    int num_threads;
    
    if (!parseArgs(argc, argv, r_name, start_date, end_date, num_threads, table_path, result_path)) {
        std::cerr << "Failed to parse command line arguments." << std::endl;
        return 1;
    }
    
    std::cout << "Loading TPCH data from: " << table_path << std::endl;
    std::vector<std::map<std::string, std::string>> customer_data, orders_data, lineitem_data, supplier_data, nation_data, region_data;
    
    if (!readTPCHData(table_path, customer_data, orders_data, lineitem_data, supplier_data, nation_data, region_data)) {
        std::cerr << "Failed to read TPCH data." << std::endl;
        return 1;
    }
    
    std::cout << "Data loaded successfully!" << std::endl;
    std::cout << "Executing Query 5 with " << num_threads << " thread(s)..." << std::endl;
    
    std::map<std::string, double> results;
    
    if (!executeQuery5(r_name, start_date, end_date, num_threads, customer_data, orders_data, lineitem_data, supplier_data, nation_data, region_data, results)) {
        std::cerr << "Failed to execute TPCH Query 5." << std::endl;
        return 1;
    }
    
    if (!outputResults(result_path, results)) {
        std::cerr << "Failed to output results." << std::endl;
        return 1;
    }
    
    // Optional: Display execution time
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    std::cout << "Execution time: " << duration.count() << " ms" << std::endl;
    
    std::cout << "TPCH Query 5 implementation completed." << std::endl;
    std::cout << "Results saved to: " << result_path << std::endl;
    
    return 0;
}

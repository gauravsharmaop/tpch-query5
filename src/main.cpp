#include "query5.hpp"
#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <unordered_map>

// No need for legacy includes like <map>, <sstream>, etc.

int main(int argc, char* argv[]) {
    std::string r_name, start_date, end_date, table_path, result_path;
    int num_threads;

    // Parse command line arguments (optimized version)
    if (!parseArgs(argc, argv, r_name, start_date, end_date, num_threads, table_path, result_path)) {
        std::cerr << "Error parsing arguments\n";
        return 1;
    }

    // Load data into optimized structures
    std::vector<Customer> customers;
    std::vector<Order> orders;
    std::vector<LineItem> lineitems;
    std::vector<Supplier> suppliers;
    std::vector<Nation> nations;
    std::vector<Region> regions;

    if (!readOptimizedData(table_path, customers, orders, lineitems, suppliers, nations, regions)) {
        std::cerr << "Error reading data files\n";
        return 1;
    }

    // Execute optimized query
    std::unordered_map<std::string, double> results;
    if (!executeOptimizedQuery5(r_name, start_date, end_date, num_threads,
                                customers, orders, lineitems, suppliers, nations, regions, results)) {
        std::cerr << "Error executing query\n";
        return 1;
    }

    // Output results
    if (!outputResults(result_path, results)) {
        std::cerr << "Error writing results\n";
        return 1;
    }

    return 0;
}

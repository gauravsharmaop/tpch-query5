#include "query5.hpp"
#include <iostream>
#include <fstream>
#include <sstream>
#include <thread>
#include <mutex>
#include <iomanip>
#include <algorithm>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <string_view>

// Global mutex for thread-safe result aggregation
std::mutex result_mutex;

// Optimized structures using minimal memory
struct Customer {
    uint32_t custkey;
    uint16_t nationkey;
    
    Customer(uint32_t ck, uint16_t nk) : custkey(ck), nationkey(nk) {}
};

struct Order {
    uint32_t orderkey;
    uint32_t custkey;
    uint32_t orderdate; // Store as days since epoch for faster comparison
    
    Order(uint32_t ok, uint32_t ck, uint32_t od) : orderkey(ok), custkey(ck), orderdate(od) {}
};

struct LineItem {
    uint32_t orderkey;
    uint32_t suppkey;
    double revenue; // Pre-calculate revenue to avoid repeated computation
    
    LineItem(uint32_t ok, uint32_t sk, double rev) : orderkey(ok), suppkey(sk), revenue(rev) {}
};

struct Supplier {
    uint32_t suppkey;
    uint16_t nationkey;
    
    Supplier(uint32_t sk, uint16_t nk) : suppkey(sk), nationkey(nk) {}
};

struct Nation {
    uint16_t nationkey;
    uint16_t regionkey;
    std::string name;
    
    Nation(uint16_t nk, uint16_t rk, std::string n) : nationkey(nk), regionkey(rk), name(std::move(n)) {}
};

struct Region {
    uint16_t regionkey;
    std::string name;
    
    Region(uint16_t rk, std::string n) : regionkey(rk), name(std::move(n)) {}
};

// Fast date conversion (YYYY-MM-DD to days since epoch)
uint32_t dateToInt(const std::string& date) {
    if (date.length() < 10) return 0;
    
    int year = std::stoi(date.substr(0, 4));
    int month = std::stoi(date.substr(5, 2));
    int day = std::stoi(date.substr(8, 2));
    
    // Simple approximation: days since 1990-01-01
    return (year - 1990) * 365 + (month - 1) * 30 + day;
}

// Optimized string splitting with string_view
std::vector<std::string_view> splitView(std::string_view str, char delimiter) {
    std::vector<std::string_view> result;
    size_t start = 0;
    size_t pos = 0;
    
    while ((pos = str.find(delimiter, start)) != std::string_view::npos) {
        result.emplace_back(str.substr(start, pos - start));
        start = pos + 1;
    }
    
    if (start < str.length()) {
        result.emplace_back(str.substr(start));
    }
    
    return result;
}

// Function to parse command line arguments (unchanged for compatibility)
bool parseArgs(int argc, char* argv[], std::string& r_name, std::string& start_date, std::string& end_date, int& num_threads, std::string& table_path, std::string& result_path) {
    r_name = "ASIA";
    start_date = "1994-01-01";
    end_date = "1995-01-01";
    num_threads = 1;
    table_path = "./";
    result_path = "./results.txt";
    
    for (int i = 1; i < argc; i += 2) {
        if (i + 1 >= argc) break;
        
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
            if (table_path.back() != '/') {
                table_path += "/";
            }
        } else if (arg == "--result_path") {
            result_path = value;
        }
    }
    
    return true;
}

// Optimized data reading functions
bool readOptimizedData(const std::string& table_path,
                      std::vector<Customer>& customers,
                      std::vector<Order>& orders,
                      std::vector<LineItem>& lineitems,
                      std::vector<Supplier>& suppliers,
                      std::vector<Nation>& nations,
                      std::vector<Region>& regions) {
    
    // Pre-allocate vectors with estimated sizes
    customers.reserve(150000);
    orders.reserve(1500000);
    lineitems.reserve(6000000);
    suppliers.reserve(10000);
    nations.reserve(25);
    regions.reserve(5);
    
    // Read customers
    std::ifstream file(table_path + "customer.tbl");
    if (!file.is_open()) return false;
    
    std::string line;
    while (std::getline(file, line)) {
        auto tokens = splitView(line, '|');
        if (tokens.size() >= 4) {
            customers.emplace_back(
                std::stoul(std::string(tokens[0])),
                std::stoul(std::string(tokens[3]))
            );
        }
    }
    file.close();
    
    // Read orders
    file.open(table_path + "orders.tbl");
    if (!file.is_open()) return false;
    
    while (std::getline(file, line)) {
        auto tokens = splitView(line, '|');
        if (tokens.size() >= 5) {
            orders.emplace_back(
                std::stoul(std::string(tokens[0])),
                std::stoul(std::string(tokens[1])),
                dateToInt(std::string(tokens[4]))
            );
        }
    }
    file.close();
    
    // Read lineitems with pre-calculated revenue
    file.open(table_path + "lineitem.tbl");
    if (!file.is_open()) return false;
    
    while (std::getline(file, line)) {
        auto tokens = splitView(line, '|');
        if (tokens.size() >= 7) {
            double extendedprice = std::stod(std::string(tokens[5]));
            double discount = std::stod(std::string(tokens[6]));
            double revenue = extendedprice * (1.0 - discount);
            
            lineitems.emplace_back(
                std::stoul(std::string(tokens[0])),
                std::stoul(std::string(tokens[2])),
                revenue
            );
        }
    }
    file.close();
    
    // Read suppliers
    file.open(table_path + "supplier.tbl");
    if (!file.is_open()) return false;
    
    while (std::getline(file, line)) {
        auto tokens = splitView(line, '|');
        if (tokens.size() >= 4) {
            suppliers.emplace_back(
                std::stoul(std::string(tokens[0])),
                std::stoul(std::string(tokens[3]))
            );
        }
    }
    file.close();
    
    // Read nations
    file.open(table_path + "nation.tbl");
    if (!file.is_open()) return false;
    
    while (std::getline(file, line)) {
        auto tokens = splitView(line, '|');
        if (tokens.size() >= 3) {
            nations.emplace_back(
                std::stoul(std::string(tokens[0])),
                std::stoul(std::string(tokens[2])),
                std::string(tokens[1])
            );
        }
    }
    file.close();
    
    // Read regions
    file.open(table_path + "region.tbl");
    if (!file.is_open()) return false;
    
    while (std::getline(file, line)) {
        auto tokens = splitView(line, '|');
        if (tokens.size() >= 2) {
            regions.emplace_back(
                std::stoul(std::string(tokens[0])),
                std::string(tokens[1])
            );
        }
    }
    file.close();
    
    return true;
}

// Optimized worker function using hash maps for O(1) lookups
void processOrdersChunkOptimized(
    const std::vector<Order>& orders_chunk,
    uint32_t start_date_int,
    uint32_t end_date_int,
    const std::unordered_map<uint32_t, uint16_t>& customer_nation_map,
    const std::unordered_map<uint32_t, std::vector<size_t>>& order_lineitem_map,
    const std::vector<LineItem>& lineitems,
    const std::unordered_map<uint32_t, uint16_t>& supplier_nation_map,
    const std::unordered_map<uint16_t, std::string>& nation_names,
    const std::unordered_set<uint16_t>& target_nations,
    std::unordered_map<std::string, double>& global_results) {
    
    std::unordered_map<std::string, double> local_results;
    
    for (const auto& order : orders_chunk) {
        // Quick date filter
        if (order.orderdate < start_date_int || order.orderdate >= end_date_int) {
            continue;
        }
        
        // Quick customer nation lookup
        auto cust_it = customer_nation_map.find(order.custkey);
        if (cust_it == customer_nation_map.end()) continue;
        
        uint16_t cust_nation = cust_it->second;
        if (target_nations.find(cust_nation) == target_nations.end()) continue;
        
        // Process lineitems for this order
        auto lineitem_it = order_lineitem_map.find(order.orderkey);
        if (lineitem_it != order_lineitem_map.end()) {
            for (size_t idx : lineitem_it->second) {
                const auto& lineitem = lineitems[idx];
                
                auto supp_it = supplier_nation_map.find(lineitem.suppkey);
                if (supp_it != supplier_nation_map.end() && supp_it->second == cust_nation) {
                    const std::string& nation_name = nation_names.at(cust_nation);
                    local_results[nation_name] += lineitem.revenue;
                }
            }
        }
    }
    
    // Thread-safe merge
    std::lock_guard<std::mutex> lock(result_mutex);
    for (const auto& [nation, revenue] : local_results) {
        global_results[nation] += revenue;
    }
}

// Optimized main query execution
bool executeOptimizedQuery5(const std::string& r_name,
                           const std::string& start_date,
                           const std::string& end_date,
                           int num_threads,
                           const std::vector<Customer>& customers,
                           const std::vector<Order>& orders,
                           const std::vector<LineItem>& lineitems,
                           const std::vector<Supplier>& suppliers,
                           const std::vector<Nation>& nations,
                           const std::vector<Region>& regions,
                           std::unordered_map<std::string, double>& results) {
    
    results.clear();
    
    // Build optimized lookup structures
    std::unordered_map<uint32_t, uint16_t> customer_nation_map;
    std::unordered_map<uint32_t, uint16_t> supplier_nation_map;
    std::unordered_map<uint16_t, std::string> nation_names;
    std::unordered_set<uint16_t> target_nations;
    std::unordered_map<uint32_t, std::vector<size_t>> order_lineitem_map;
    
    // Pre-size hash maps
    customer_nation_map.reserve(customers.size());
    supplier_nation_map.reserve(suppliers.size());
    order_lineitem_map.reserve(orders.size());
    
    // Build customer map
    for (const auto& customer : customers) {
        customer_nation_map[customer.custkey] = customer.nationkey;
    }
    
    // Build supplier map
    for (const auto& supplier : suppliers) {
        supplier_nation_map[supplier.suppkey] = supplier.nationkey;
    }
    
    // Build nation names map
    for (const auto& nation : nations) {
        nation_names[nation.nationkey] = nation.name;
    }
    
    // Find target region and nations
    uint16_t target_regionkey = UINT16_MAX;
    for (const auto& region : regions) {
        if (region.name == r_name) {
            target_regionkey = region.regionkey;
            break;
        }
    }
    
    if (target_regionkey == UINT16_MAX) return false;
    
    for (const auto& nation : nations) {
        if (nation.regionkey == target_regionkey) {
            target_nations.insert(nation.nationkey);
        }
    }
    
    // Build order->lineitem mapping for faster joins
    for (size_t i = 0; i < lineitems.size(); ++i) {
        order_lineitem_map[lineitems[i].orderkey].push_back(i);
    }
    
    // Convert dates
    uint32_t start_date_int = dateToInt(start_date);
    uint32_t end_date_int = dateToInt(end_date);
    
    // Execute query with threading
    if (num_threads == 1) {
        processOrdersChunkOptimized(orders, start_date_int, end_date_int,
                                  customer_nation_map, order_lineitem_map, lineitems,
                                  supplier_nation_map, nation_names, target_nations, results);
    } else {
        std::vector<std::thread> threads;
        size_t chunk_size = orders.size() / num_threads;
        
        for (int i = 0; i < num_threads; ++i) {
            size_t start_idx = i * chunk_size;
            size_t end_idx = (i == num_threads - 1) ? orders.size() : (i + 1) * chunk_size;
            
            std::vector<Order> orders_chunk(orders.begin() + start_idx, orders.begin() + end_idx);
            
            threads.emplace_back(processOrdersChunkOptimized, 
                               std::move(orders_chunk), start_date_int, end_date_int,
                               std::ref(customer_nation_map), std::ref(order_lineitem_map), 
                               std::ref(lineitems), std::ref(supplier_nation_map),
                               std::ref(nation_names), std::ref(target_nations), std::ref(results));
        }
        
        for (auto& thread : threads) {
            thread.join();
        }
    }
    
    return true;
}

// Optimized output function
bool outputResults(const std::string& result_path, const std::unordered_map<std::string, double>& results) {
    std::ofstream output_file(result_path);
    if (!output_file.is_open()) {
        std::cerr << "Error: Cannot open output file " << result_path << std::endl;
        return false;
    }
    
    // Sort results by revenue (descending)
    std::vector<std::pair<std::string, double>> sorted_results(results.begin(), results.end());
    std::sort(sorted_results.begin(), sorted_results.end(),
              [](const auto& a, const auto& b) { return a.second > b.second; });
    
    // Output results
    std::cout << "\nQuery Results:\n";
    std::cout << "Nation\t\tRevenue\n";
    std::cout << "------\t\t-------\n";
    
    output_file << "Nation\tRevenue\n";
    for (const auto& [nation, revenue] : sorted_results) {
        std::cout << nation << "\t\t" << std::fixed << std::setprecision(2) << revenue << '\n';
        output_file << nation << '\t' << revenue << '\n';
    }
    
    output_file.close();
    return true;
}

// Main function with optimized flow
int main(int argc, char* argv[]) {
    std::string r_name, start_date, end_date, table_path, result_path;
    int num_threads;
    
    if (!parseArgs(argc, argv, r_name, start_date, end_date, num_threads, table_path, result_path)) {
        std::cerr << "Error parsing arguments\n";
        return 1;
    }
    
    // Load data with optimized structures
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

#include "query5.hpp"
#include <iostream>
#include <fstream>
#include <sstream>
#include <thread>
#include <mutex>
#include <algorithm>

// Mutex for thread-safe writing
std::mutex result_mutex;

// Function to parse command line arguments
bool parseArgs(int argc, char* argv[], std::string& r_name, std::string& start_date, std::string& end_date, int& num_threads, std::string& table_path, std::string& result_path) {
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--r_name" && i + 1 < argc) {
            r_name = argv[++i];
        } else if (arg == "--start_date" && i + 1 < argc) {
            start_date = argv[++i];
        } else if (arg == "--end_date" && i + 1 < argc) {
            end_date = argv[++i];
        } else if (arg == "--threads" && i + 1 < argc) {
            num_threads = std::stoi(argv[++i]);
        } else if (arg == "--table_path" && i + 1 < argc) {
            table_path = argv[++i];
        } else if (arg == "--result_path" && i + 1 < argc) {
            result_path = argv[++i];
        } else {
            std::cerr << "Unknown or incomplete argument: " << arg << std::endl;
            return false;
        }
    }

    if (r_name.empty() || start_date.empty() || end_date.empty() || num_threads <= 0 || table_path.empty() || result_path.empty()) {
        std::cerr << "Missing or invalid argument." << std::endl;
        return false;
    }

    return true;
}

// Simple function to read a TPCH table
void readTable(const std::string& filename, std::vector<std::map<std::string, std::string>>& table_data, const std::vector<std::string>& headers) {
    std::ifstream file(filename);
    if (!file.is_open()) {
        std::cerr << "Failed to open file: " << filename << std::endl;
        return;
    }

    std::string line;
    while (std::getline(file, line)) {
        std::map<std::string, std::string> row;
        std::stringstream ss(line);
        std::string value;
        int i = 0;

        while (std::getline(ss, value, '|') && i < headers.size()) {
            row[headers[i]] = value;
            i++;
        }
        table_data.push_back(row);
    }

    file.close();
}

// Function to read TPCH data from the specified paths
bool readTPCHData(const std::string& table_path, std::vector<std::map<std::string, std::string>>& customer_data, std::vector<std::map<std::string, std::string>>& orders_data, std::vector<std::map<std::string, std::string>>& lineitem_data, std::vector<std::map<std::string, std::string>>& supplier_data, std::vector<std::map<std::string, std::string>>& nation_data, std::vector<std::map<std::string, std::string>>& region_data) {
    try {
        readTable(table_path + "/customer.tbl", customer_data, {"c_custkey", "c_nationkey"});
        readTable(table_path + "/orders.tbl", orders_data, {"o_orderkey", "o_custkey", "o_orderdate"});
        readTable(table_path + "/lineitem.tbl", lineitem_data, {"l_orderkey", "l_suppkey", "l_extendedprice", "l_discount"});
        readTable(table_path + "/supplier.tbl", supplier_data, {"s_suppkey", "s_nationkey"});
        readTable(table_path + "/nation.tbl", nation_data, {"n_nationkey", "n_name", "n_regionkey"});
        readTable(table_path + "/region.tbl", region_data, {"r_regionkey", "r_name"});
    } catch (...) {
        return false;
    }
    return true;
}

// Worker function for multithreading
void queryWorker(const std::string& r_name, const std::string& start_date, const std::string& end_date,
                 const std::vector<std::map<std::string, std::string>>& customer_data,
                 const std::vector<std::map<std::string, std::string>>& orders_data,
                 const std::vector<std::map<std::string, std::string>>& lineitem_data,
                 const std::vector<std::map<std::string, std::string>>& supplier_data,
                 const std::vector<std::map<std::string, std::string>>& nation_data,
                 const std::vector<std::map<std::string, std::string>>& region_data,
                 std::map<std::string, double>& results, size_t start, size_t end) {

    std::map<std::string, double> local_results;

    for (size_t i = start; i < end; ++i) {
        const auto& order = orders_data[i];
        std::string o_orderdate = order.at("o_orderdate");

        if (o_orderdate < start_date || o_orderdate >= end_date)
            continue;

        std::string o_custkey = order.at("o_custkey");
        std::string o_orderkey = order.at("o_orderkey");

        auto cust_it = std::find_if(customer_data.begin(), customer_data.end(), [&](const auto& cust) {
            return cust.at("c_custkey") == o_custkey;
        });

        if (cust_it == customer_data.end())
            continue;

        std::string cust_nationkey = cust_it->at("c_nationkey");

        auto supp_it = std::find_if(supplier_data.begin(), supplier_data.end(), [&](const auto& supp) {
            return supp.at("s_nationkey") == cust_nationkey;
        });

        if (supp_it == supplier_data.end())
            continue;

        std::string supp_nationkey = supp_it->at("s_nationkey");

        auto nation_it = std::find_if(nation_data.begin(), nation_data.end(), [&](const auto& nation) {
            return nation.at("n_nationkey") == supp_nationkey;
        });

        if (nation_it == nation_data.end())
            continue;

        std::string nation_name = nation_it->at("n_name");
        std::string nation_regionkey = nation_it->at("n_regionkey");

        auto region_it = std::find_if(region_data.begin(), region_data.end(), [&](const auto& region) {
            return region.at("r_regionkey") == nation_regionkey && region.at("r_name") == r_name;
        });

        if (region_it == region_data.end())
            continue;

        double revenue = 0.0;

        for (const auto& line : lineitem_data) {
            if (line.at("l_orderkey") == o_orderkey) {
                double price = std::stod(line.at("l_extendedprice"));
                double discount = std::stod(line.at("l_discount"));
                revenue += price * (1 - discount);
            }
        }

        local_results[nation_name] += revenue;
    }

    // Lock before writing to shared results
    std::lock_guard<std::mutex> lock(result_mutex);
    for (const auto& entry : local_results) {
        results[entry.first] += entry.second;
    }
}

// Function to execute TPCH Query 5 using multithreading
bool executeQuery5(const std::string& r_name, const std::string& start_date, const std::string& end_date, int num_threads, const std::vector<std::map<std::string, std::string>>& customer_data, const std::vector<std::map<std::string, std::string>>& orders_data, const std::vector<std::map<std::string, std::string>>& lineitem_data, const std::vector<std::map<std::string, std::string>>& supplier_data, const std::vector<std::map<std::string, std::string>>& nation_data, const std::vector<std::map<std::string, std::string>>& region_data, std::map<std::string, double>& results) {

    std::vector<std::thread> threads;
    size_t chunk_size = orders_data.size() / num_threads;

    for (int i = 0; i < num_threads; ++i) {
        size_t start = i * chunk_size;
        size_t end = (i == num_threads - 1) ? orders_data.size() : (i + 1) * chunk_size;

        threads.emplace_back(queryWorker, r_name, start_date, end_date,
                             std::cref(customer_data), std::cref(orders_data), std::cref(lineitem_data),
                             std::cref(supplier_data), std::cref(nation_data), std::cref(region_data),
                             std::ref(results), start, end);
    }

    for (auto& t : threads) {
        t.join();
    }

    return true;
}

// Function to output results to the specified path
bool outputResults(const std::string& result_path, const std::map<std::string, double>& results) {
    std::ofstream outfile(result_path);
    if (!outfile.is_open()) {
        std::cerr << "Failed to write to file: " << result_path << std::endl;
        return false;
    }

    for (const auto& entry : results) {
        outfile << entry.first << " | " << entry.second << std::endl;
    }

    outfile.close();
    return true;
}

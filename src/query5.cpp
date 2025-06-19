#include "query5.hpp"
#include <iostream>
#include <fstream>
#include <sstream>
#include <thread>
#include <mutex>
#include <algorithm>
#include <map>
#include <vector>

// Mutex for thread-safe writing
std::mutex result_mutex;

// parseArgs and readTPCHData remain unchanged

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

        // Find the customer
        auto cust_it = std::find_if(customer_data.begin(), customer_data.end(), [&](const auto& cust) {
            return cust.at("c_custkey") == o_custkey;
        });
        if (cust_it == customer_data.end())
            continue;

        std::string cust_nationkey = cust_it->at("c_nationkey");

        // Find the nation of the customer
        auto nation_it = std::find_if(nation_data.begin(), nation_data.end(), [&](const auto& nation) {
            return nation.at("n_nationkey") == cust_nationkey;
        });
        if (nation_it == nation_data.end())
            continue;

        std::string nation_name = nation_it->at("n_name");
        std::string nation_regionkey = nation_it->at("n_regionkey");

        // Find the region
        auto region_it = std::find_if(region_data.begin(), region_data.end(), [&](const auto& region) {
            return region.at("r_regionkey") == nation_regionkey && region.at("r_name") == r_name;
        });
        if (region_it == region_data.end())
            continue;

        double revenue = 0.0;

        // Iterate over lineitems for this order
        for (const auto& line : lineitem_data) {
            if (line.at("l_orderkey") == o_orderkey) {
                std::string l_suppkey = line.at("l_suppkey");

                // Find the supplier for this lineitem
                auto supp_it = std::find_if(supplier_data.begin(), supplier_data.end(), [&](const auto& supp) {
                    return supp.at("s_suppkey") == l_suppkey && supp.at("s_nationkey") == cust_nationkey;
                });
                if (supp_it == supplier_data.end())
                    continue;

                double price = std::stod(line.at("l_extendedprice"));
                double discount = std::stod(line.at("l_discount"));
                revenue += price * (1 - discount);
            }
        }

        if (revenue > 0.0)
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
    std::vector<std::pair<std::string, double>> sorted_results(results.begin(), results.end());

    // Sort by revenue descending
    std::sort(sorted_results.begin(), sorted_results.end(), [](const auto& a, const auto& b) {
        return a.second > b.second;
    });

    std::ofstream outfile(result_path);
    if (!outfile.is_open()) {
        std::cerr << "Failed to write to file: " << result_path << std::endl;
        return false;
    }

    for (const auto& entry : sorted_results) {
        outfile << entry.first << " | " << entry.second << std::endl;
    }

    outfile.close();
    return true;
}

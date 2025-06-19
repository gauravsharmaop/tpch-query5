#ifndef QUERY5_HPP
#define QUERY5_HPP

#include <string>
#include <vector>
#include <unordered_map>

// Optimized structures for TPC-H Query 5
struct Customer {
    uint32_t custkey;
    uint16_t nationkey;
    Customer(uint32_t ck, uint16_t nk) : custkey(ck), nationkey(nk) {}
};

struct Order {
    uint32_t orderkey;
    uint32_t custkey;
    uint32_t orderdate; // days since epoch
    Order(uint32_t ok, uint32_t ck, uint32_t od) : orderkey(ok), custkey(ck), orderdate(od) {}
};

struct LineItem {
    uint32_t orderkey;
    uint32_t suppkey;
    double revenue; // extendedprice * (1 - discount)
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

// Function declarations for the optimized implementation
bool parseArgs(int argc, char* argv[], std::string& r_name, std::string& start_date, std::string& end_date, int& num_threads, std::string& table_path, std::string& result_path);

bool readOptimizedData(const std::string& table_path,
                      std::vector<Customer>& customers,
                      std::vector<Order>& orders,
                      std::vector<LineItem>& lineitems,
                      std::vector<Supplier>& suppliers,
                      std::vector<Nation>& nations,
                      std::vector<Region>& regions);

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
                           std::unordered_map<std::string, double>& results);

bool outputResults(const std::string& result_path, const std::unordered_map<std::string, double>& results);

#endif // QUERY5_HPP

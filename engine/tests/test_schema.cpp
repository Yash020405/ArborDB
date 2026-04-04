#include "../src/schema/schema.h"
#include <cassert>
#include <iostream>
#include <filesystem>

using namespace arbor;

int main() {
    const std::string testDir = "/tmp/arbor_schema_test";
    std::filesystem::remove_all(testDir);

    SchemaManager mgr(testDir);

    assert(!mgr.tableExists("users"));

    TableSchema schema;
    schema.tableName = "users";
    schema.primaryKey = "id";
    schema.columns = {{"id", ColumnType::INT}, {"name", ColumnType::STRING}, {"score", ColumnType::FLOAT}};
    mgr.createTable(schema);

    assert(mgr.tableExists("users"));

    bool threw = false;
    try { mgr.createTable(schema); } catch (...) { threw = true; }
    assert(threw);

    {
        SchemaManager mgr2(testDir);
        TableSchema loaded = mgr2.loadTable("users");
        assert(loaded.tableName == "users");
        assert(loaded.primaryKey == "id");
        assert(loaded.columns.size() == 3);
        assert(loaded.columns[0].name == "id");
        assert(loaded.columns[0].type == ColumnType::INT);
        assert(loaded.columns[1].name == "name");
        assert(loaded.columns[1].type == ColumnType::STRING);
        assert(loaded.columns[2].name == "score");
        assert(loaded.columns[2].type == ColumnType::FLOAT);

        auto tables = mgr2.listTables();
        assert(tables.size() == 1);
        assert(tables[0] == "users");
    }

    assert(parseColumnType("INT")    == ColumnType::INT);
    assert(parseColumnType("STRING") == ColumnType::STRING);
    assert(parseColumnType("FLOAT")  == ColumnType::FLOAT);
    assert(parseColumnType("BOOL")   == ColumnType::BOOL);
    assert(columnTypeToString(ColumnType::INT) == "INT");

    std::filesystem::remove_all(testDir);
    std::cout << "PASS: all schema tests passed\n";
    return 0;
}

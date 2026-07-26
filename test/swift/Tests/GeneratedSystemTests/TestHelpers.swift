@testable import GeneratedSystem
import Foundation
import ProprDBSwiftRuntime

func makePerson(name: String = "", age: Int64 = 0) -> Generatedtest_Example_Person {
    var person = Generatedtest_Example_Person()
    person.name = name
    person.age = age
    return person
}

func makeNote(text: String = "") -> Generatedtest_Example_Note {
    var note = Generatedtest_Example_Note()
    note.text = text
    return note
}

func jsonString(_ value: String) -> String {
    let data = try! JSONEncoder().encode(value)
    return String(decoding: data, as: UTF8.self)
}

func jsonlInputStream(_ text: String) -> InputStream {
    InputStream(data: Data(text.utf8))
}

func tableIndexNamesByName(db: SQLiteDatabase, tableName: String) throws -> Set<String> {
    try db.withRows("SELECT name FROM pragma_index_list(\(quoteSQLiteString(tableName)))") { rows in
        var indexes = Set<String>()
        while let row = try rows.next() {
            indexes.insert(try row.string(at: 0))
        }
        return indexes
    }
}

func scalarInt(_ db: SQLiteDatabase, sql: String, arguments: [Any?]) throws -> Int64 {
    try db.withRows(sql, arguments: arguments) { rows in
        guard let row = try rows.next() else {
            return 0
        }
        return try row.int64(at: 0)
    }
}

func scalarString(_ db: SQLiteDatabase, sql: String, arguments: [Any?]) throws -> String {
    try db.withRows(sql, arguments: arguments) { rows in
        guard let row = try rows.next() else {
            return ""
        }
        return try row.string(at: 0)
    }
}

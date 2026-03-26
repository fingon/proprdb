import Foundation
import ProprDBSwiftRuntime

extension Generatedtest_Example_Person: ProprDBValidatable {
    public func valid() throws {
        if name.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
            throw ProprDBError("name is required")
        }
    }
}

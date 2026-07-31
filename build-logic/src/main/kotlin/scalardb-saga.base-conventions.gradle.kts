// Project identity shared by every module. Separate from java-conventions because the BOM is a
// `java-platform`, which cannot also be a `java-library`, yet still needs the same coordinates.
//
// The Maven group is deliberately not the Java package (com.scalar.db.saga): it matches the other
// Scalar artifacts on Central (com.scalar-labs:scalardb, com.scalar-labs:scalardb-cluster-*), and a
// groupId cannot be changed after the first release without orphaning every published version.
group = "com.scalar-labs"

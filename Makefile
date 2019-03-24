all: Hw1Grp0.class

Hw1Grp0.class: Hw1Grp0.java
	javac $^

run: Hw1Grp0.class
	java Hw1Grp0 R=/hw1/lineitem.tbl S=/hw1/orders.tbl join:R0=S0 res:S1,R1,R5

test: Hw1Grp0.class
	java Hw1Grp0 R=/tpch/customer.tbl S=/tpch/supplier.tbl join:R0=S0 res:R1,S1

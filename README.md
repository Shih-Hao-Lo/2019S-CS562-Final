## Extended SQL Processor
### CS562A Final Project

### Group Members
1. Shih-Hao Lo
2. Wei-Hsuan Wong

#### Description
Extended SQL is a new processing way to help the database management system to process mutiple join operations at the same time with linear time complexity.

#### Syntax

> 1. Select: The columns to output.
> 2. From: Table name.
> 3. Where: Condition for the data row.
> 4. Group by(Part 1): The attribute which need to be group together.
> 5. Group by(Part 2): How many different conditions user wamt to compute.
> 5. Such that: An ESQL syntax which check the conditions for each group.

#### Example
>  select cust,month, sum(x.quant), sum(y.quant),count(y.quant),avg(y.quant)  
> from sales  
> where year = 1995  
> Group by cust,month; x, y  
> such that x.month <= month , y.month >= month  

#### How to use
> 1. Put the ESQL query in /src/esql.txt file.
> 2. Run /src/input.java file to produce the /src/output.java file.
> 3. Run /src/output.java file and the outpute will be on the console.

#### Attanetion
> 1. Must include PostgreSQL JDBC classpath.
> 2. The Syntax must match the example.

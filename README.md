# Join_Size_Estimator
Calculates the difference between estimated and actual join sizes in a relational database. Its a java program and it uses jdbc to connect to the database, retrieve necessary data and perform appropriate calculations.


Brief Description of problem statement:

Write a Java program that takes the name of two table names of the dataset as
command-line arguments. It should then connect to the database, calculate and print the following:
1. Estimated Join Size: Estimate the size of the natural join of the two tables.
2. Actual Join Size: Determine the size of actual natural join of the two tables.
3. Estimation Error : The difference between the estimation and actual size (Estimated Join Size
− Actual Join Size). The error will be positive when you over-estimate and negative when
you under-estimate the size.

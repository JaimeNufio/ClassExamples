REGISTER file:/Users/yaoshen/Documents/Misc/Teaching/CS644BigData/Formal/Week9/myudfs.jar;
A = load '/input/student_data' as (name:chararray, age:int, gpa:float);
B = foreach A generate myudfs.UPPER(name);
dump B;

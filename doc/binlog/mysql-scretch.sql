show tables from maxwell;

drop database test;
create database if not exists test;

create table student (
 id int(6) primary key auto_increment comment '主键',
 name varchar(50) comment '姓名'
);

insert into student(name) values ('a1'),('a2');
update student set name='a11' where id=1;
delete from student where id=1;

select * from student;


insert into student(name) values ('a3'),('a4');

update student set name='a33' where id=3;
update student set name='a44' where id=4;

select * from student;



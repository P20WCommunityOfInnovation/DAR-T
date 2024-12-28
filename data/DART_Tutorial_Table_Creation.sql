USE SI
GO

DROP TABLE IF EXISTS [dbo].[DAT_Tutorial]
CREATE TABLE [dbo].[DAT_Tutorial](
OrganizationName varchar(255),
OrganizationID bigint,
GroupType varchar(255),
GroupName varchar(255),
TotalStudents bigint,
NumberGraduated bigint,
GraduationRate decimal(9,6)
) ON [PRIMARY]

INSERT INTO [dbo].[DAT_Tutorial]
VALUES ('School One', 999991, 'Income', 'Low Income', 95, 61, 0.642105)
INSERT INTO [dbo].[DAT_Tutorial]
VALUES ('School One', 999991, 'Income','Non Low Income', 27, 9, 0.333333)
INSERT INTO [dbo].[DAT_Tutorial]
VALUES ('School One', 999991,'All', 'All Students', 122, 70, 0.573770)
INSERT INTO [dbo].[DAT_Tutorial]
VALUES ('School Two', 999992, 'Income','Low Income', 37, 15, 0.405405)
INSERT INTO [dbo].[DAT_Tutorial]
VALUES ('School Two', 999992, 'Income','Non Low Income', 43, 30, 0.697674)
INSERT INTO [dbo].[DAT_Tutorial]
VALUES ('School Two', 999992, 'All','All Students', 80, 45, 0.562500)
INSERT INTO [dbo].[DAT_Tutorial]
VALUES ('School Three', 999993, 'Income', 'Low Income', 17, 16, 0.941176)
INSERT INTO [dbo].[DAT_Tutorial]
VALUES ('School Three', 999993, 'Income','Non Low Income', 12, 1, 0.083333)
INSERT INTO [dbo].[DAT_Tutorial]
VALUES ('School Three', 999993, 'All', 'All Students', 29, 17, 0.586207)
INSERT INTO [dbo].[DAT_Tutorial]
VALUES ('School Four', 999994, 'Income','Low Income', 75, 13, 0.173333)
INSERT INTO [dbo].[DAT_Tutorial]
VALUES ('School Four', 999994, 'Income','Non Low Income', 12, 9, 0.750000)
INSERT INTO [dbo].[DAT_Tutorial]
VALUES ('School Four', 999994, 'All', 'All Students', 87, 22, 0.252874)
INSERT INTO [dbo].[DAT_Tutorial]
VALUES ('School Five', 999995, 'Income','Low Income', 79, 78, 0.987342)
INSERT INTO [dbo].[DAT_Tutorial]
VALUES ('School Five', 999995, 'Income','Non Low Income', 20, 5, 0.250000)
INSERT INTO [dbo].[DAT_Tutorial]
VALUES ('School Five', 999995, 'All', 'All Students', 99, 83, 0.838384)
INSERT INTO [dbo].[DAT_Tutorial]
VALUES ('School Six', 999996, 'Income','Low Income', 11, 2, 0.181818)
INSERT INTO [dbo].[DAT_Tutorial]
VALUES ('School Six', 999996, 'Income','Non Low Income', 17, 14, 0.823529)
INSERT INTO [dbo].[DAT_Tutorial]
VALUES ('School Six', 999996, 'All', 'All Students', 28, 16, 0.571429)
INSERT INTO [dbo].[DAT_Tutorial]
VALUES ('School Seven', 999997, 'Income','Low Income', 14, 3, 0.214286)
INSERT INTO [dbo].[DAT_Tutorial]
VALUES ('School Seven', 999997, 'Income','Non Low Income', 19, 0, 0.000000)
INSERT INTO [dbo].[DAT_Tutorial]
VALUES ('School Seven', 999997, 'All', 'All Students', 33, 3, 0.090909)
INSERT INTO [dbo].[DAT_Tutorial]
VALUES ('School Eight', 999998, 'Income','Low Income', 35, 20, 0.571429)
INSERT INTO [dbo].[DAT_Tutorial]
VALUES ('School Eight', 999998, 'Income','Non Low Income', 34, 32, 0.941176)
INSERT INTO [dbo].[DAT_Tutorial]
VALUES ('School Eight', 999998, 'All', 'All Students', 69, 52, 0.753623)
INSERT INTO [dbo].[DAT_Tutorial]
VALUES ('School Nine', 999999, 'Income','Low Income', 12, 11, 0.916667)
INSERT INTO [dbo].[DAT_Tutorial]
VALUES ('School Nine', 999999, 'Income','Non Low Income', 10, 0, 0.000000)
INSERT INTO [dbo].[DAT_Tutorial]
VALUES ('School Nine', 999999, 'All', 'All Students', 22, 11, 0.500000)
INSERT INTO [dbo].[DAT_Tutorial]
VALUES ('School Ten', 999910, 'Income','Low Income', 80, 47, 0.587500)
INSERT INTO [dbo].[DAT_Tutorial]
VALUES ('School Ten', 999910, 'Income','Non Low Income', 122, 121, 0.991803)
INSERT INTO [dbo].[DAT_Tutorial]
VALUES ('School Ten', 999910, 'All', 'All Students', 202, 168, 0.831683)
INSERT INTO [dbo].[DAT_Tutorial]
VALUES ('School Eleven', 999911, 'Income','Low Income', 2579, 2323, 0.900737)
INSERT INTO [dbo].[DAT_Tutorial]
VALUES ('School Eleven', 999911, 'Income','Non Low Income', 2544, 1614, 0.634434)
INSERT INTO [dbo].[DAT_Tutorial]
VALUES ('School Eleven', 999911, 'All', 'All Students', 5123, 3937, 0.768495)
INSERT INTO [dbo].[DAT_Tutorial]
VALUES ('School Twelve', 999912, 'Income', 'Low Income', 43997, 33130, 0.753000)
INSERT INTO [dbo].[DAT_Tutorial]
VALUES ('School Twelve', 999912, 'Income', 'Non Low Income', 41243, 0, 0)
INSERT INTO [dbo].[DAT_Tutorial]
VALUES ('School Twelve', 999912, 'All', 'All Students', 85240, 33130, 0.388667)


SELECT * FROM [dbo].[DAT_Tutorial]
package com.puneet.mysql;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.sql.*;

public class Main {

    public static void main(String[] args) {

        Connection connection = null;
        final String updateStatement = "insert into tbl_large_dataset (data) values (?)";
        try {
            Class.forName("com.mysql.jdbc.Driver");

            connection = DriverManager.getConnection(
                                    "jdbc:mysql://10.2.117.134:3306/large_dataset",
                                    "root",
                                    "Oracle@123");

            final PreparedStatement preparedStatement = connection.prepareStatement(updateStatement);
            final String fileName = "/home/puneet/Downloads/johndoe_resume.pdf";

            final File file = new File(fileName);
            final FileInputStream fileInputStream = new FileInputStream(file);

            preparedStatement.setBinaryStream(1, fileInputStream);

            System.out.println("Starting the loop");
            for(int i=0; i<10000; i++) {
                preparedStatement.executeUpdate();
            }
            System.out.println("Data inserted successfully");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}

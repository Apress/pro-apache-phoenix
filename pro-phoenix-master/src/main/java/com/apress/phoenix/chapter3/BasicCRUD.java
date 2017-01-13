package com.apress.phoenix.chapter3;

import com.apress.phoenix.common.Application;
import com.apress.phoenix.common.Queries;
import lombok.extern.slf4j.Slf4j;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 *
 */
@Slf4j
public class BasicCRUD extends Application {

    public BasicCRUD()  {
        super();
    }

    public static void main(String args[])
    {
        new BasicCRUD().doCrud();
    }

    public void doCrud() {
        try {
            start();
            final String createQuery = Queries.sql("chapter3/user-create.sql");
            final Statement statement = this.connection.createStatement();
            statement.executeUpdate(createQuery);

            // upsert rows
            statement.executeUpdate("upsert into user values (1,'shakil', 'soz')");
            statement.executeUpdate("upsert into user values (2,'ravi', 'magham')");

            this.connection.commit();

            // fetch rows
            final PreparedStatement pstmt = this.connection.prepareStatement("select first_name from user");
            final ResultSet rset = pstmt.executeQuery();
            while (rset.next()) {
                System.out.println(rset.getString("first_name"));
            }

        } catch(Exception ex) {
            final String errorMsg = String.format("Error [%s] occurred while performing crud ", ex.getMessage());
            log.error(errorMsg, ex);
        } finally {
            close();
        }
    }
}

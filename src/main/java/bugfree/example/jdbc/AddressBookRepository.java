/*
 * Copyright (C) 2024 Stefano Fornari.
 *
 * All Rights Reserved.  No use, copying or distribution of this
 * work may be made except in accordance with a valid license
 * agreement from Stefano Fornari.  This notice must be
 * included on all copies, modifications and derivatives of this
 * work.
 *
 * STEFANO FORNARI MAKES NO REPRESENTATIONS OR WARRANTIES ABOUT THE SUITABILITY
 * OF THE SOFTWARE, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
 * THE IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR
 * PURPOSE, OR NON-INFRINGEMENT. STEFANO FORNARI SHALL NOT BE LIABLE FOR ANY
 * DAMAGES SUFFERED BY LICENSEE AS A RESULT OF USING, MODIFYING OR DISTRIBUTING
 * THIS SOFTWARE OR ITS DERIVATIVES.
 */
package bugfree.example.jdbc;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class AddressBookRepository {
    private final String connectionString;

    public AddressBookRepository(final String connectionString) {
        this.connectionString = connectionString;
    }

    private void createTable() throws SQLException {
        try (Connection c = DriverManager.getConnection(connectionString)) {
            Statement stmt = c.createStatement();
            stmt.execute(
                "CREATE TABLE contacts (" +
                "id INT AUTO_INCREMENT PRIMARY KEY, " +
                "first_name VARCHAR(50) NOT NULL, " +
                "last_name VARCHAR(50) NOT NULL, " +
                "phone_number VARCHAR(20), " +
                "email VARCHAR(100)" +
                ")"
            );
        }
    }

    public Contact add(Contact contact) throws SQLException {
        final String sqlInsert
            = "INSERT INTO contacts (email, first_name, last_name, phone_number) VALUES (?, ?, ?, ?) IF NOT EXISTS";

        try (
            Connection connection = DriverManager.getConnection(connectionString);
            PreparedStatement insert = connection.prepareStatement(sqlInsert);
        ) {
            insert.setString(1, contact.getEmail());
            insert.setString(2, contact.getFirstName());
            insert.setString(3, contact.getLastName());
            insert.setString(4, contact.getPhoneNumber());

            insert.executeUpdate();
            ResultSet rs = insert.getResultSet();
            if ((rs != null) && rs.next() && !rs.getBoolean(1)) {
                throw new SQLException("row already exists");
            }
        }

        return contact;
    }

    public Contact save(final Contact contact) throws SQLException {
        final String sqlInsert
            = "UPDATE contacts SET first_name=?, last_name=?, phone_number=? WHERE email=?";

        try (
            Connection connection = DriverManager.getConnection(connectionString);
            PreparedStatement insert = connection.prepareStatement(sqlInsert);
        ) {

            insert.setString(1, contact.getFirstName());
            insert.setString(2, contact.getLastName());
            insert.setString(3, contact.getPhoneNumber());
            insert.setString(4, contact.getEmail());

            insert.executeUpdate();
        }

        return contact;
    }

    public List<Contact> getAllContacts() throws SQLException {
        List<Contact> contacts = new ArrayList<>();
        String sql = "SELECT * FROM contacts";

        try (
            Connection connection = DriverManager.getConnection(connectionString);
            PreparedStatement ps = connection.prepareStatement(sql);
            ResultSet rs = ps.executeQuery(sql)
        ) {

            while (rs.next()) {
                Contact contact = new Contact(
                    rs.getString("first_name"),
                    rs.getString("last_name"),
                    rs.getString("phone_number"),
                    rs.getString("email")
                );
                contact.setId(rs.getLong("id"));
                contacts.add(contact);
            }
        }

        return contacts;
    }
}

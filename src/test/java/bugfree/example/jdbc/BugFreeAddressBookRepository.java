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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.List;
import static org.assertj.core.api.BDDAssertions.then;
import static org.assertj.core.api.BDDAssertions.thenThrownBy;
import org.junit.BeforeClass;
import org.junit.Test;
import ste.xtest.jdbc.Column;
import ste.xtest.jdbc.CompositeHandler;
import ste.xtest.jdbc.QueryResult;
import ste.xtest.jdbc.RowList;
import ste.xtest.jdbc.StatementHandler;
import ste.xtest.jdbc.StatementHandler.Parameter;
import ste.xtest.jdbc.UpdateResult;

/**
 *
 */
public class BugFreeAddressBookRepository {

    public static final String CONNECTION_STRING = "jdbc:h2:mem:addressbook";
    public static final String STUB_CONNECTION_STRING = "jdbc:xtest:address-book?handler=insert-if-not-exists";

    @BeforeClass
    public static void before_all() throws Exception {
        final Connection connection = connect();

        Statement stmt = connection.createStatement();
        stmt.execute(
                "CREATE TABLE contacts ("
                + "id INT AUTO_INCREMENT PRIMARY KEY, "
                + "first_name VARCHAR(50) NOT NULL, "
                + "last_name VARCHAR(50) NOT NULL, "
                + "phone_number VARCHAR(20), "
                + "email VARCHAR(100)"
                + ")"
        );

        StatementHandler handler = new CompositeHandler()
                .withQueryDetection("^SELECT ", "^select ")
                .withUpdateHandler(new CompositeHandler.UpdateHandler() {
                    // Handle execution of update statement (not query)
                    public UpdateResult apply(String query, List<Parameter> parameters) throws SQLException {
                        if (query.contains(" IF NOT EXISTS")) {
                            String selectSql = "SELECT email FROM contacts WHERE email=?";

                            boolean alreadyExist = false;
                            RowList rs = new RowList(new Column(Boolean.class, "[applied]"));
                            try (PreparedStatement select = connection.prepareStatement(selectSql)) {
                                select.setObject(1, parameters.getFirst().right);

                                alreadyExist = select.executeQuery().next();
                                if (!alreadyExist) {
                                    query = query.replace(" IF NOT EXISTS", ""); // make it standard SQL
                                    try (PreparedStatement insert = connection.prepareStatement(query)) {
                                        int i = 1;
                                        for (Parameter p : parameters) {
                                            insert.setObject(i++, p.right);
                                        }
                                        insert.executeUpdate();
                                    }
                                }
                            }
                            rs.append(List.of(!alreadyExist));
                            return new UpdateResult((alreadyExist ? 0 : 1), rs, null, null);
                        } else {
                            // normal statement
                            try (PreparedStatement stmt = connection.prepareStatement(query)) {
                                int i = 1;
                                for (Parameter p : parameters) {
                                    stmt.setObject(i++, p.right);
                                }
                                return new UpdateResult(stmt.executeUpdate(), stmt.getResultSet(), (RowList) null, (SQLWarning) null);
                            }
                        }
                    }
                })
                .withQueryHandler(new CompositeHandler.QueryHandler() {
                    @Override
                    public QueryResult apply(String sql, List<Parameter> parameters) throws SQLException {
                        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
                            int i = 1;
                            for (Parameter p : parameters) {
                                stmt.setObject(i++, p.right);
                            }
                            return new QueryResult(stmt.executeQuery(), null);
                        }
                    }
                });

        // Register prepared handler with expected ID 'insert-or-update'
        ste.xtest.jdbc.XDriver.register("insert-if-not-exists", handler);
    }

    @Test
    public void add_throws_exception_if_exists() throws Exception {
        final Contact JOHN_DOE
                = new Contact("John", "Doe", "+1 111 1234567", "john.doe@somewhere.com");

        AddressBookRepository ab = new AddressBookRepository(STUB_CONNECTION_STRING);

        ab.add(JOHN_DOE); // insert

        Connection connection = connect();

        PreparedStatement s = connection.prepareStatement(
            "SELECT * FROM contacts WHERE email=?"
        );
        s.setString(1, JOHN_DOE.getEmail());
        ResultSet rs = s.executeQuery();
        rs.next();

        then(rs.getString(2)).isEqualTo(JOHN_DOE.getFirstName());
        then(rs.getString(3)).isEqualTo(JOHN_DOE.getLastName());
        then(rs.getString(4)).isEqualTo(JOHN_DOE.getPhoneNumber());
        then(rs.getString(5)).isEqualTo(JOHN_DOE.getEmail());
        then(rs.next()).isFalse(); // only one record

        thenThrownBy(() -> {
            ab.add(JOHN_DOE); // record already exists!
        }).isInstanceOf(SQLException.class).hasMessage("row already exists");
    }

    @Test
    public void update_existing_record() throws Exception {
        final Contact JOHN_DOE
                = new Contact("John", "Doe", "+1 111 1234567", "john.doe@somewhere.com");

        AddressBookRepository ab = new AddressBookRepository(STUB_CONNECTION_STRING);

        ab.save(JOHN_DOE); // update
        JOHN_DOE.setPhoneNumber("+1 222 33 444 20");
        ab.save(JOHN_DOE);

        PreparedStatement s = connect().prepareStatement(
            "SELECT * FROM contacts WHERE email=?"
        );
        s.setString(1, JOHN_DOE.getEmail());
        ResultSet rs = s.executeQuery();
        rs.next();

        then(rs.getString(2)).isEqualTo(JOHN_DOE.getFirstName());
        then(rs.getString(3)).isEqualTo(JOHN_DOE.getLastName());
        then(rs.getString(4)).isEqualTo(JOHN_DOE.getPhoneNumber());
        then(rs.getString(5)).isEqualTo(JOHN_DOE.getEmail());
        then(rs.next()).isFalse(); // only one record
    }

    // ---------------------------------------------------------- private method
    private static Connection connect() throws SQLException {
        return DriverManager.getConnection(CONNECTION_STRING);
    }

}

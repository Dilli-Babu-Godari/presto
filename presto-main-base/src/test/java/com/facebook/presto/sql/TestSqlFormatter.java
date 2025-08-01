/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.sql;

import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Statement;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.sql.SqlFormatterUtil.getFormattedSql;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

public class TestSqlFormatter
{
    @Test
    public void testSimpleExpression()
    {
        assertQuery("SELECT id\nFROM\n  public.orders\n");
        assertQuery("SELECT id\nFROM\n  \"public\".\"order\"\n");
        assertQuery("SELECT id\nFROM\n  \"public\".\"order\"\"2\"\n");
    }

    @Test
    public void testQuotedColumnNames()
    {
        assertQuery("SELECT \"Id\"\nFROM\n  public.orders\n");
        assertQuery("SELECT \"order\".\"Name\"\nFROM\n  \"public\".\"orders\"\n");
        assertQuery("SELECT \"a\".\"b\".\"C\"\nFROM\n  \"schema\".\"table\"\n");
        assertQuery("ALTER TABLE sales.orders RENAME COLUMN \"OrderId\" TO \"OrderID_New\"");
        assertQuery("ALTER TABLE sales.orders ADD COLUMN \"Customer\" VARCHAR");
        assertQuery("ALTER TABLE sales.orders DROP COLUMN \"Customer\"");
    }

    private void assertQuery(String query)
    {
        SqlParser parser = new SqlParser();
        Statement statement = parser.createStatement(query, new ParsingOptions());
        String formattedQuery = getFormattedSql(statement, parser, Optional.empty());
        assertEquals(formattedQuery, query);
        assertEquals(formattedQuery, query, format("Formatted SQL did not match original for query: %s", query));
    }
}

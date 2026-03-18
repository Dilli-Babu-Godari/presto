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
package com.facebook.presto.tests;

import com.facebook.presto.Session;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Tests for the time_zone_id session property.
 */
public class TestTimeZoneSessionProperty
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return DistributedQueryRunner.builder(testSessionBuilder().build()).build();
    }

    @Test
    public void testSetTimeZoneIdWithNamedZone()
    {
        MaterializedResult result = computeActual("SET SESSION time_zone_id = 'America/Los_Angeles'");
        assertEquals(result.getRowCount(), 1);
        assertTrue(result.toString().contains("setSessionProperties={time_zone_id=America/Los_Angeles}"));
    }

    @Test
    public void testSetTimeZoneIdWithOffset()
    {
        MaterializedResult result = computeActual("SET SESSION time_zone_id = '+05:30'");
        assertEquals(result.getRowCount(), 1);
        assertTrue(result.toString().contains("setSessionProperties={time_zone_id=+05:30}"));
    }

    @DataProvider(name = "invalidTimezones")
    public Object[][] invalidTimezonesProvider()
    {
        return new Object[][] {
                {"Wrong/Zone"},
                {"-24:30"},
                {"+25:00"},
                {"Invalid/Timezone"},
                {"America/InvalidCity"}
        };
    }

    @Test(dataProvider = "invalidTimezones")
    public void testInvalidTimeZoneThrowsError(String timezone)
    {
        assertQueryFails("SET SESSION time_zone_id = '" + timezone + "'", ".*Invalid time zone.*");
    }

    @DataProvider(name = "validTimezoneValues")
    public Object[][] currentTimezoneValuesProvider()
    {
        return new Object[][] {
                {"America/Los_Angeles"},
                {"+05:30"},
                {"UTC"}
        };
    }

    @Test(dataProvider = "validTimezoneValues")
    public void testCurrentTimezoneReflectsUpdatedValue(String timezone)
    {
        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty("time_zone_id", timezone)
                .build();

        MaterializedResult result = computeActual(session, "SELECT current_timezone()");
        String actualTimezone = (String) result.getMaterializedRows().get(0).getField(0);
        assertEquals(actualTimezone, timezone);
    }
}

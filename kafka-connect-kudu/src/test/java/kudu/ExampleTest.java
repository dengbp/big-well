package kudu;

/**
 * @author dengbp
 * @ClassName ExampleTest
 * @Description TODO
 * @date 2020-05-08 13:39
 */
import static org.junit.Assert.assertTrue;

import org.apache.kudu.client.KuduException;
import org.apache.kudu.test.KuduTestHarness;
import org.junit.Rule;
import org.junit.Test;

public class ExampleTest {

    /**
     * A Junit Rule that manages a Kudu cluster and clients for testing.
     * This rule also includes utility methods for the cluster
     * and clients.
     */
    @Rule
    public KuduTestHarness harness = new KuduTestHarness();

    @Test
    public void testCreateExampleTable() throws KuduException {
        String tableName = "test_create_example";
        Example.createExampleTable(harness.getClient(), tableName);
        assertTrue(harness.getClient().tableExists(tableName));
    }
}

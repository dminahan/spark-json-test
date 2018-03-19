package dminahan.spark.json;

import java.io.Serializable;

import org.junit.Test;

import com.holdenkarau.spark.testing.JavaDatasetSuiteBase;

public class TestJsonMain extends JavaDatasetSuiteBase implements Serializable {

	@Test
    public void test() {
          new JsonMain().run(spark());
    }
}

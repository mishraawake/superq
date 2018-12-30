package org.apache.superq.db;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class FileMetadataTest extends AbstractTest {


  @Before
  public void setup(){

  }

  @Test
  public void testMetadataSave() throws IOException {
    try {
      FileMetadata fileMedadata = new FileMetadata(TEST_DATA_DIRECTORY, "metadata");
      fileMedadata.setProcessedSize(10);
      fileMedadata.setSize(100000);

      Assert.assertEquals(fileMedadata.getSize() , 100000);
      Assert.assertEquals(fileMedadata.getProcessedSize() , 10);
    } finally {
      Files.delete(Paths.get(TEST_DATA_DIRECTORY + "/metadata"));
    }
  }
}

// Copyright 2017 Amazon Web Services, Inc. or its affiliates. All rights reserved.

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;

import java.io.File;
import java.io.IOException;

public class Utils {
  public static Region getRegion() {
    Region region = Regions.getCurrentRegion();

    // For local testing only
    if (region == null) {
      region = Region.getRegion(Regions.EU_WEST_1);
    }

    System.out.printf("Get region returns: %s%n", region.getName());
    return region;
  }

  static AWSCredentials getCredentials() {
    try {
      return new PropertiesCredentials(
        new File("/.aws/credentials"));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}

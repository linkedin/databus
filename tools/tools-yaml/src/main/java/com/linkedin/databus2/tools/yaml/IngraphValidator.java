package com.linkedin.databus2.tools.yaml;

import java.io.FileReader;

import org.yaml.snakeyaml.Yaml;

public class IngraphValidator
{
  public static void main(String[] args) throws Exception
  {
    Yaml yaml = new Yaml();
    Object result = yaml.load(new FileReader(args[0]));
    System.out.println(result);
  }

}

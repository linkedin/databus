package com.linkedin.databus.core.util;

import static org.testng.AssertJUnit.assertEquals;

import java.beans.PropertyDescriptor;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.beanutils.BeanUtilsBean;
import org.apache.commons.beanutils.ConvertUtilsBean;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestConfigManager
{

static
{
  LogFactory.getFactory().setAttribute("org.apache.commons.logging.Log", Log4JLogger.class.getName());

  //Logger.getRootLogger().setLevel(Level.TRACE);
  Logger.getRootLogger().setLevel(Level.ERROR);
}

  private DynamicConfigBuilder _configBuilder;
  private MyConfigManager _configManager;

  @BeforeMethod
  public void setUp() throws Exception
  {
    _configBuilder = new DynamicConfigBuilder();
    _configManager = new MyConfigManager(_configBuilder);
  }

  @AfterMethod
  public void tearDown() throws Exception
  {
  }

  @Test
  public void testLoadConfig_HappyPath() throws Exception
  {

    Properties props = new Properties();
    props.setProperty("com.linkedin.databus2.intSetting", "1");
    props.setProperty("com.linkedin.databus2.nestedConfig.stringSetting", "Hello");
    props.setProperty("com.linkedin.databus2.listSetting[0].stringSetting", "item0");
    props.setProperty("com.linkedin.databus2.listSetting[1].stringSetting", "item1");
    props.setProperty("com.linkedin.databus.intSetting", "2");

    _configManager.loadConfig(props);

    DynamicConfig dynConfig = _configManager.getReadOnlyConfig();
    assertEquals("intSetting correct", 1, dynConfig.getIntSetting());
    assertEquals("nestedConfig.stringSetting correct", "Hello",
                 dynConfig.getNestedConfig().getStringSetting());
    assertEquals("listSetting size correct", 2, dynConfig.getListSetting().size());
    assertEquals("listSetting[0].stringSetting", "item0",
                 dynConfig.getListSetting(0).getStringSetting());
    assertEquals("listSetting[1].stringSetting", "item1",
                 dynConfig.getListSetting(1).getStringSetting());

    _configManager.setSetting("com.linkedin.databus2.intSetting", 3);
    _configManager.setSetting("com.linkedin.databus2.listSetting[2].stringSetting", "item2");
    assertEquals("old intSetting correct", 1, dynConfig.getIntSetting());
    assertEquals("listSetting size correct", 2, dynConfig.getListSetting().size());

    DynamicConfig newDynConfig = _configManager.getReadOnlyConfig();
    assertEquals("new intSetting correct", 3, newDynConfig.getIntSetting());
    assertEquals("nestedConfig.stringSetting correct", "Hello",
                 newDynConfig.getNestedConfig().getStringSetting());
    assertEquals("listSetting size correct", 3, newDynConfig.getListSetting().size());
    assertEquals("listSetting[0].stringSetting", "item0",
                 newDynConfig.getListSetting(0).getStringSetting());
    assertEquals("listSetting[1].stringSetting", "item1",
                 newDynConfig.getListSetting(1).getStringSetting());
    assertEquals("listSetting[2].stringSetting", "item2",
                 newDynConfig.getListSetting(2).getStringSetting());
  }

  @Test(expectedExceptions=InvalidConfigException.class)
  public void testInvalidSimpleProperty() throws InvalidConfigException
  {
    _configManager.setSetting("com.linkedin.databus2.inSetting", 3);
  }

  @Test(expectedExceptions=InvalidConfigException.class)
  public void testInvalidNestedProperty1() throws InvalidConfigException
  {
    Properties props = new Properties();
    props.setProperty("com.linkedin.databus2.nestedConfig.inSetting", "1");
    _configManager.loadConfig(props);
  }

  @Test(expectedExceptions=InvalidConfigException.class)
  public void testInvalidNestedProperty2() throws InvalidConfigException
  {
    Properties props = new Properties();
    props.setProperty("com.linkedin.databus2.nestdConfig.stringSetting", "1");
    _configManager.loadConfig(props);
  }

  @Test(expectedExceptions=InvalidConfigException.class)
  public void testInvalidNestedProperty3() throws InvalidConfigException
  {
    Properties props = new Properties();
    props.setProperty("com.linkedin.databus2.lstConfig[1].stringSetting", "1");
    _configManager.loadConfig(props);
  }

  @Test(expectedExceptions=InvalidConfigException.class)
  public void testInvalidNestedProperty4() throws InvalidConfigException
  {
    Properties props = new Properties();
    props.setProperty("com.linkedin.databus2.listSetting[1].inSetting", "1");
    _configManager.loadConfig(props);
  }

  @Test
  public void testIgnoreSetting() throws InvalidConfigException
  {
    Properties props = new Properties();
    props.setProperty("com.linkedin.databs2.listSetting[1].stringSetting", "1");
    _configManager.loadConfig(props);
  }

  @Test
  public void testLoadConfigFromJson_HappyPath() throws Exception
  {
    String json = "{\"intSetting\": 1,\"nestedConfig\":{\"stringSetting\":\"Hello\"},\"listSetting\":[{\"stringSetting\":\"item0\"},{\"stringSetting\":\"item1\"}]}";
    StringReader jsonReader = new StringReader(json);

    _configManager.loadConfigFromJson(jsonReader);

    DynamicConfig dynConfig = _configManager.getReadOnlyConfig();
    assertEquals("intSetting correct", 1, dynConfig.getIntSetting());
    assertEquals("nestedConfig.stringSetting correct", "Hello",
                 dynConfig.getNestedConfig().getStringSetting());
    assertEquals("listSetting size correct", 2, dynConfig.getListSetting().size());
    assertEquals("listSetting[0].stringSetting", "item0",
                 dynConfig.getListSetting(0).getStringSetting());
    assertEquals("listSetting[1].stringSetting", "item1",
                 dynConfig.getListSetting(1).getStringSetting());
  }

  @Test
  public void testPaddedInt() throws Exception
  {
    ConvertUtilsBean convertUtils = new ConvertUtilsBean();
    Integer intValue = (Integer)convertUtils.convert("456", int.class);

    assertEquals("correct int value", 456, intValue.intValue());
    BeanUtilsBean beanUtils = new BeanUtilsBean();
    PropertyDescriptor propDesc = beanUtils.getPropertyUtils().getPropertyDescriptor(_configBuilder,
                                                                                     "intSetting");
    assertEquals("correct setting type", int.class, propDesc.getPropertyType());

    _configManager.setSetting("com.linkedin.databus2.intSetting", " 123 ");
    DynamicConfig config = _configManager.getReadOnlyConfig();
    assertEquals("correct int value", 123, config.getIntSetting());
  }

  @Test
  public void testStringPropertyTrimming() throws Exception
  {
   DynamicConfig config =
     _configManager.setSetting("com.linkedin.databus2.nestedConfig.stringSetting", " text to trim  ");
   assertEquals("trimmed value", "text to trim", config.getNestedConfig().getStringSetting());
  }

  public static class NestedDynamicConfig implements ConfigApplier<NestedDynamicConfig>
  {
    private final String _stringSetting;

    public NestedDynamicConfig(String stringSetting)
    {
      _stringSetting = stringSetting;
    }

    public String getStringSetting()
    {
      return _stringSetting;
    }

    @Override
    public void applyNewConfig(NestedDynamicConfig oldConfig)
    {
      // TODO Auto-generated method stub
    }

    @Override
    public boolean equals(Object otherConfig)
    {
      if (null == otherConfig || !(otherConfig instanceof NestedDynamicConfig)) return false;
      return equalsConfig((NestedDynamicConfig)otherConfig);
    }

    @Override
    public boolean equalsConfig(NestedDynamicConfig otherConfig)
    {
      if (null == otherConfig) return false;
      return getStringSetting().equals(otherConfig.getStringSetting());
    }

    @Override
    public int hashCode()
    {
      return _stringSetting.hashCode();
    }

  }

  public static class NestedDynamicConfigBuilder implements ConfigBuilder<NestedDynamicConfig>
  {
    private String _stringSetting;

    public NestedDynamicConfigBuilder()
    {
      super();
      _stringSetting = "default";
    }

    public String getStringSetting()
    {
      return _stringSetting;
    }

    public void setStringSetting(String stringSetting)
    {
      _stringSetting = stringSetting;
    }

    @Override
    public NestedDynamicConfig build() throws InvalidConfigException
    {
      if (null == _stringSetting)
      {
        throw new InvalidConfigException("Null string setting");
      }
      else
      {
        return new NestedDynamicConfig(_stringSetting);
      }
    }

  }

  public static class DynamicConfig implements ConfigApplier<DynamicConfig>
  {
    private final int _intSetting;
    private final NestedDynamicConfig _nestedBean;
    private final ArrayList<NestedDynamicConfig> _listSetting;

    public DynamicConfig(int intSetting, NestedDynamicConfig nestedBean,
                         ArrayList<NestedDynamicConfig> listSetting)
    {
      _intSetting = intSetting;
      _nestedBean = nestedBean;
      _listSetting = listSetting;
    }

    public int getIntSetting()
    {
      return _intSetting;
    }

    public NestedDynamicConfig getNestedConfig()
    {
      return _nestedBean;
    }

    public NestedDynamicConfig getListSetting(int i)
    {
      return _listSetting.get(i);
    }

    public List<NestedDynamicConfig> getListSetting()
    {
      return _listSetting;
    }

    @Override
    public void applyNewConfig(DynamicConfig oldConfig)
    {
      if (null != getNestedConfig())
      {
        getNestedConfig().applyNewConfig(null != oldConfig ? oldConfig.getNestedConfig() : null);
      }
      // FIXME Auto-generated method stub
    }

    @Override
    public boolean equals(Object otherConfig)
    {
      if (null == otherConfig || !(otherConfig instanceof DynamicConfig)) return false;
      return equalsConfig((DynamicConfig)otherConfig);
    }

    @Override
    public boolean equalsConfig(DynamicConfig otherConfig)
    {
      if (null == otherConfig) return false;
      return (getIntSetting() == otherConfig.getIntSetting()) &&
             (getListSetting().equals(otherConfig.getListSetting())) &&
             ((null == getNestedConfig() && null == otherConfig.getNestedConfig()) ||
              (null != getNestedConfig() && getNestedConfig().equals(otherConfig.getNestedConfig()))) ;
    }

    @Override
    public int hashCode()
    {
      return _intSetting ^ _listSetting.hashCode();
    }
  }

  public static class DynamicConfigBuilder implements ConfigBuilder<DynamicConfig>
  {
    private int _intSetting;
    private NestedDynamicConfigBuilder _nestedConfig;
    private ArrayList<NestedDynamicConfigBuilder> _listSetting;

    public DynamicConfigBuilder()
    {
      super();
      _intSetting = 0;
      _nestedConfig = new NestedDynamicConfigBuilder();
      _listSetting = new ArrayList<NestedDynamicConfigBuilder>();
    }

    public int getIntSetting()
    {
      return _intSetting;
    }

    public void setIntSetting(int intSetting)
    {
      _intSetting = intSetting;
    }

    public NestedDynamicConfigBuilder getNestedConfig()
    {
      return _nestedConfig;
    }

    public DynamicConfigBuilder setNestedConfig(NestedDynamicConfigBuilder nestedBean)
    {
      _nestedConfig = nestedBean;
      return this;
    }

    public NestedDynamicConfigBuilder getListSetting(int idx)
    {
      if (idx >= _listSetting.size())
      {
        setListSetting(idx, new NestedDynamicConfigBuilder());
      }

      return _listSetting.get(idx);
    }

    public void setListSetting(int idx, NestedDynamicConfigBuilder value)
    {
      if (idx < _listSetting.size())
      {
        _listSetting.set(idx, value);
      }
      else
      {
        for (int i = _listSetting.size(); i < idx; ++i) _listSetting.add(new NestedDynamicConfigBuilder());
        _listSetting.add(value);
      }
    }

    @Override
    public DynamicConfig build() throws InvalidConfigException
    {
      if (_listSetting.size() >= 5) throw new InvalidConfigException("Big list");
      if (_intSetting < 0) throw new InvalidConfigException("Negative int setting");

      ArrayList<NestedDynamicConfig> beanList = new ArrayList<NestedDynamicConfig>(_listSetting.size());
      for (NestedDynamicConfigBuilder builder: _listSetting) beanList.add(builder.build());
      return new DynamicConfig(_intSetting, _nestedConfig.build(), beanList);
    }
  }

  public static class MyConfigManager extends ConfigManager<DynamicConfig>
  {

    public MyConfigManager(DynamicConfigBuilder dynConfigBuilder) throws InvalidConfigException
    {
      super("com.linkedin.databus2.", dynConfigBuilder);
    }

  }
}


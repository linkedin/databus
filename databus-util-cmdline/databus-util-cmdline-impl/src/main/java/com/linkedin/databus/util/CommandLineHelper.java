/*
 * $Id: CommandLineHelper.java 152467 2010-11-24 02:43:02Z jwesterm $
 */
package com.linkedin.databus.util;
/*
 *
 * Copyright 2013 LinkedIn Corp. All rights reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
*/


import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * @author Jemiah Westerman<jwesterman@linkedin.com>
 * @version $Revision: 152467 $
 */
public class CommandLineHelper
{
  public enum ArgumentType {STRING, INTEGER, LONG, FLOAT, DOUBLE, BOOLEAN, DIRECTORY, FILE};

  private static class ArgumentInfo
  implements Comparable<ArgumentInfo>
  {
    private final String _name;
    private final boolean _required;
    private final String _description;
    private final ArgumentType _type;
    private final Pattern _regexPattern;

    public ArgumentInfo(String name, boolean required, String description, ArgumentType type, Pattern regexPattern)
    {
      _name = name;
      _required = required;
      _description = description;
      _type = type;
      _regexPattern = regexPattern;
    }

    @Override
    public int compareTo(ArgumentInfo other)
    {
      return _name.compareTo(other._name);
    }

    @Override
    public boolean equals(Object o)
    {
      if (null == o || !(o instanceof ArgumentInfo)) return false;
      return _name.equals(((ArgumentInfo)o)._name);
    }

    @Override
    public int hashCode()
    {
      return _name.hashCode();
    }
  }

  private final Map<String, ArgumentInfo> _argumentInfos = new HashMap<String, ArgumentInfo>();

  public void addArgument(String name, boolean required, String description, ArgumentType type)
  {
    addArgument(name, required, description, type, null);
  }

  public void addArgument(String name, boolean required, String description, ArgumentType type, String regexPattern)
  {
    // Argument names should not have the leading dash. We handle that internally and don't want
    // confusion from the caller whether or not the Map returned by parseCommandLine has keys with or
    // without the dash.
    if(name.startsWith("-"))
    {
      throw new IllegalArgumentException("Argument names should not begin with a leading dash (-).");
    }

    // If the optional regular expression pattern was specified, then compile the pattern here.
    Pattern pattern = null;
    if(regexPattern != null)
    {
      pattern = Pattern.compile(regexPattern);
    }

    // Create the new ArgumentInfo and add it to the map.
    // Note the key in the map has the leading dash.
    _argumentInfos.put("-" + name, new ArgumentInfo(name, required, description, type, pattern));
  }

  /**
   * Parse the command line arguments passed in the {@code args} array. Prior to calling this method,
   * all legal argument names should be registered with {@code addArgument(...)}.
   */
  public Map<String, Object> parseCommandLine(String[] args)
  {
    Map<String, Object> arguments = new HashMap<String, Object>();

    // Should be an even number of arguments in the form -argName argValue
    if(args.length % 2 != 0)
    {
      showUsage("Bad command line");
      return null;
    }

    for(int i=0; i < args.length; i+=2)
    {
      String argumentName = args[i];
      String argumentValue = args[i+1];

      // Get the ArgumentInfo for this argumentName. If it is null then it is a bad argument and we abort
      ArgumentInfo argumentInfo = _argumentInfos.get(argumentName);
      if(argumentInfo == null)
      {
        showUsage("Bad command line. The argument name does not exist: " + argumentName);
        return null;
      }

      // If the argument has the optional regular expression pattern, make sure the value provided
      // matches the pattern
      if(argumentInfo._regexPattern != null)
      {
        if(!argumentInfo._regexPattern.matcher(argumentValue).matches())
        {
          showUsage("Bad command line. The argument " + argumentName + " must match the pattern " + argumentInfo._regexPattern.toString());
          return null;
        }
      }

      // Put the argument value in the return map.
      // Do this in a try/catch block so we can capture NumberFormatException etc. if the value is
      // not valid for the specified type.
      try
      {
        switch(argumentInfo._type)
        {
        case BOOLEAN:
          arguments.put(argumentInfo._name, Boolean.valueOf(argumentValue));
          break;
        case DOUBLE:
          arguments.put(argumentInfo._name, Double.valueOf(argumentValue));
          break;
        case FLOAT:
          arguments.put(argumentInfo._name, Float.valueOf(argumentValue));
          break;
        case INTEGER:
          arguments.put(argumentInfo._name, Integer.valueOf(argumentValue));
          break;
        case LONG:
          arguments.put(argumentInfo._name, Long.valueOf(argumentValue));
          break;
        case STRING:
          arguments.put(argumentInfo._name, argumentValue);
          break;
        case FILE:
          File file = new File(argumentValue);
          if(!file.exists() || file.isDirectory())
          {
            showUsage("Bad command line. The file " + argumentValue + " does not exist.");
            return null;
          }
          arguments.put(argumentInfo._name, file);
          break;
        case DIRECTORY:
          File dir = new File(argumentValue);
          if(!dir.exists() || !dir.isDirectory())
          {
            showUsage("Bad command line. The directory " + argumentValue + " does not exist.");
            return null;
          }
          arguments.put(argumentInfo._name, dir);
          break;
        }
      }
      catch(Exception ex)
      {
        showUsage("Bad command line. The argument " + argumentName + " had a bad value. It must be of type " + argumentInfo._type);
        return null;
      }
    }

    boolean missingRequiredArgs = false;
    for(ArgumentInfo argumentInfo : _argumentInfos.values())
    {
      if(argumentInfo._required && !arguments.containsKey(argumentInfo._name))
      {
        System.out.println("Missing required argument: " + argumentInfo._name);
        missingRequiredArgs = true;
      }
    }
    if(missingRequiredArgs)
    {
      showUsage();
      return null;
    }

    // Return the map of parsed arguments
    return arguments;
  }


  public void showParsedArguments(Map<String, Object> arguments)
  {
    showParsedArguments(null, arguments);
  }

  public void showParsedArguments(String message, Map<String, Object> arguments)
  {
    // Print the optional message if one was provided, followed by our normal usage header
    if(message != null)
    {
      System.out.println(message);
    }

    // Print all the arguments that were passed in on the command line
    for(Map.Entry<String, Object> entry : arguments.entrySet())
    {
      System.out.println(entry.getKey() + "=" + entry.getValue());
    }
  }

  public void showUsage()
  {
    showUsage(null);
  }

  public void showUsage(String message)
  {
    // Build a sorted list of ArgumentInfos from the _argumentInfos map
    List<ArgumentInfo> argumentInfosList = new ArrayList<ArgumentInfo>();
    argumentInfosList.addAll(_argumentInfos.values());
    Collections.sort(argumentInfosList);

    // Print the optional message if one was provided, followed by our normal usage header
    if(message != null)
    {
      System.out.println(message);
    }
    System.out.println("Arguments: ");

    // Print out the required arguments first
    for(ArgumentInfo argumentInfo : argumentInfosList)
    {
      if(argumentInfo._required)
      {
        System.out.println("\t-" + argumentInfo._name + " (required): " + argumentInfo._description + " (" + argumentInfo._type + ")");
      }
    }
    // Then print out the optional arguments
    for(ArgumentInfo argumentInfo : argumentInfosList)
    {
      if(!argumentInfo._required)
      {
        System.out.println("\t-" + argumentInfo._name + " (optional): " + argumentInfo._description + " (" + argumentInfo._type + ")");
      }
    }
  }
}

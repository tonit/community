/**
 * Copyright (c) 2002-2012 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.neo4j.kernel;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.Map;
import java.util.logging.Logger;

/**
 * This class is used to create dynamic proxies that implement a configuration interface. Method calls are mapped to
 * a backing Map with String settings. If the return type is not a string, conversion is attempted. If the setting
 * does not exist, or the conversion fails, a default can be supplied as the first method argument. Example:
 * boolean read_only(boolean def) -> lookup "read_only" in map and return def if not found
 *
 * For numbers, max and min values can be specified as follows:
 * float someValue(float def, float min, float max)
 *
 * If no default is supplied, then failure to look up value, or failure to convert it, will cause exceptions. Please
 * provide defaults as often as possible to avoid this.
 */
public class ConfigProxy
    implements InvocationHandler
{

    private static final Logger log = Logger.getLogger(ConfigProxy.class.getName());
    
    public static <T> T config(Map<String, String> parameters, Class<T> configInterface)
    {
        return configInterface.cast(Proxy.newProxyInstance(configInterface.getClassLoader(), new Class<?>[]{configInterface}, new ConfigProxy(parameters)));
    }
    
    public static Map<String, String> map(Object configuration)
    {
        return ((ConfigProxy)Proxy.getInvocationHandler(configuration)).parameters;
    }

    private Map<String, String> parameters;

    public ConfigProxy( Map<String, String> parameters )
    {
        this.parameters = parameters;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable
    {
        String configName = method.getName();

        ConfigurationPrefix annotation = method.getDeclaringClass().getAnnotation( ConfigurationPrefix.class );
        String prefix = annotation == null ? "" : annotation.value();

        String key = prefix + configName;
        String val = parameters.get( key );
        
        // No value in given map for this key - use default if possible
        if (val == null)
        {
            if (args != null && args.length > 0)
                return args[0];
            else
                throw new IllegalArgumentException("Missing configuration parameter for "+method.getDeclaringClass().getName()+":"+key);
        }
        
        Object result = val;
        
        //Convert if necessary
        Class<?> returnType = method.getReturnType();
        if (!returnType.equals(String.class))
        {
            try
            {
                if (returnType.equals(Boolean.class) || returnType.equals(Boolean.TYPE))
                    result = val.equalsIgnoreCase( "true" ) || val.equalsIgnoreCase( "yes" ); // Support both true/false and yes/no
                else if (returnType.equals(Integer.class) || returnType.equals(Integer.TYPE))
                    result = rangeCheck( Integer.parseInt( val ), args );
                else if (returnType.equals(int[].class))
                {
                    String[] range = val.split( "-" );
                    if (range.length == 1)
                        result = new int[]{rangeCheck(Integer.parseInt( val ), args), rangeCheck(Integer.parseInt( val ), args)};
                    else
                    {
                        int[] ints = { rangeCheck( Integer.parseInt( range[ 0 ] ), args ), rangeCheck( Integer.parseInt( range[ 1 ] ), args ) };
                        if (ints[1]<ints[0])
                            throw new IllegalArgumentException( "Value for configuration parameter '"+key+"' is not valid:"+val+". First number must be smaller than second number");

                        result = ints;
                    }
                }
                else if (returnType.equals(Long.class) || returnType.equals(Long.TYPE))
                    result = rangeCheck(Long.parseLong( val ), args);
                else if (returnType.equals(Float.class) || returnType.equals(Float.TYPE))
                    result = rangeCheck(Float.parseFloat( val ), args);
                else if (returnType.equals(Double.class) || returnType.equals(Double.TYPE))
                    result = rangeCheck(Double.parseDouble( val ), args);
                else if (returnType.isEnum())
                {
                    try
                    {
                        result = Enum.valueOf((Class<Enum>) returnType, val.toLowerCase());
                    }
                    catch( IllegalArgumentException e )
                    {
                        String options = Arrays.asList( ((Object[])returnType.getMethod( "values" ).invoke( null )) ).toString();
                        if (args != null && args.length > 0)
                        {
                            log.warning("Value for configuration parameter '"+key+"' is not valid:"+val+". Please use one of "+options+". Using default instead:"+args[0]);
                            result = args[0];
                        } else
                        {
                            throw new IllegalArgumentException( "Value for configuration parameter '"+key+"' is not valid:"+val+". Please use one of "+options);
                        }
                    }
                }
            } catch (NumberFormatException e)
            {
                // Number has wrong format. If default exists, log warning and use default, otherwise rethrow
                if (args != null && args.length > 0)
                {
                    log.warning("Number for configuration parameter '"+key+"' has wrong format:"+val+" Using default instead:"+args[0]);
                    result = args[0];
                } else
                {
                    throw e;
                }
            }

        }

        // Default just return the value - might blow up though
        return result;
    }
    
    private <T extends Number> T rangeCheck(T result, Object[] args)
    {
        if (args != null && args.length == 3)
        {
            if (result instanceof Float)
            {
                if (((Float) result).compareTo((Float) args[1]) < 0)
                    result = (T) args[1];
                else if (((Float) result).compareTo((Float) args[2]) > 0)
                    result = (T) args[2];
            } else if (result instanceof Double)
            {
                if (((Double) result).compareTo((Double) args[1]) < 0)
                    result = (T)args[1];
                else if (((Double) result).compareTo((Double) args[2]) > 0)
                    result = (T)args[2];
            } else if (result instanceof Integer)
            {
                if (((Integer) result).compareTo((Integer) args[1]) < 0)
                    result = (T)args[1];
                else if (((Integer) result).compareTo((Integer) args[2]) > 0)
                    result = (T)args[2];
            } else if (result instanceof Long)
            {
                if (((Long) result).compareTo((Long) args[1]) < 0)
                    result = (T)args[1];
                else if (((Long) result).compareTo((Long) args[2]) > 0)
                    result = (T)args[2];
            }
        }

        return result;
    }
}

/**
 * Copyright 2008 - CommonCrawl Foundation
 * 
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the GNU General Public License as published by
 *    the Free Software Foundation, either version 3 of the License, or
 *    (at your option) any later version.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU General Public License for more details.
 *
 *    You should have received a copy of the GNU General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 **/
package org.commoncrawl.util;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.util.Collection;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.internal.Streams;
import com.google.gson.stream.JsonWriter;

/**
 * 
 * @author rana
 * 
 */
public class JSONUtils {

  public static String prettyPrintJSON(JsonElement e) throws IOException {
    StringWriter stirngWriter = new StringWriter();
    JsonWriter writer = new JsonWriter(stirngWriter);
    writer.setIndent("    ");
    writer.setHtmlSafe(true);
    writer.setLenient(true);
    Streams.write(e, writer);
    writer.flush();

    return stirngWriter.toString();
  }

  public static void safeIncrementJSONCounter(JsonObject jsonObj,
      String property) {
    if (jsonObj.has(property)) {
      jsonObj.addProperty(property, jsonObj.get(property).getAsInt() + 1);
    } else {
      jsonObj.addProperty(property, 1);
    }
  }

  public static void safeIncrementJSONCounter(JsonObject jsonObj,
      String property, JsonElement jsonElement) {
    if (jsonElement != null) {
      safeIncrementJSONCounter(jsonObj, property, jsonElement.getAsInt());
    }
  }

  public static void safeIncrementJSONCounter(JsonObject jsonObj,
      String property, int byAmount) {
    if (jsonObj.has(property)) {
      jsonObj
          .addProperty(property, jsonObj.get(property).getAsInt() + byAmount);
    } else {
      jsonObj.addProperty(property, byAmount);
    }
  }

  public static boolean safeGetBoolean(JsonObject jsonObj, String property) {
    JsonElement element = jsonObj.get(property);
    if (element != null) {
      return element.getAsBoolean();
    }
    return false;
  }

  public static long safeGetLong(JsonObject jsonObj, String property) {
    JsonElement element = jsonObj.get(property);
    if (element != null) {
      return element.getAsLong();
    }
    return -1;
  }

  public static int safeGetInteger(JsonObject jsonObj, String property) {
    JsonElement element = jsonObj.get(property);
    if (element != null) {
      return element.getAsInt();
    }
    return -1;
  }

  public static long safeGetHttpDate(JsonObject jsonObj, String property) {
    JsonElement element = jsonObj.get(property);
    if (element != null) {
      return HttpHeaderInfoExtractor.getTime(element.getAsString());
    }
    return -1;
  }

  public static void safeSetMaxLongValue(JsonObject jsonObj, String property,
      JsonElement newValue) {
    if (newValue != null) {
      safeSetMaxLongValue(jsonObj, property, newValue.getAsLong());
    }
  }

  public static long safeSetMaxLongValue(JsonObject jsonObj, String property,
      long newValue) {
    JsonElement element = jsonObj.get(property);
    if (element != null) {
      if (element.getAsLong() > newValue) {
        return element.getAsLong();
      }
    }
    jsonObj.addProperty(property, newValue);
    return newValue;
  }

  public static void safeSetMinLongValue(JsonObject jsonObj, String property,
      JsonElement newValue) {
    if (newValue != null) {
      safeSetMinLongValue(jsonObj, property, newValue.getAsLong());
    }
  }

  public static long safeSetMinLongValue(JsonObject jsonObj, String property,
      long newValue) {
    JsonElement element = jsonObj.get(property);
    if (element != null) {
      if (newValue > element.getAsLong()) {
        return element.getAsLong();
      }
    }
    jsonObj.addProperty(property, newValue);
    return newValue;
  }

  public static void safeSetStringFromElement(JsonObject jsonObj,
      String property, JsonElement stringElement) {
    if (stringElement != null) {
      jsonObj.addProperty(property, stringElement.getAsString());
    }
  }
  
  public static String safeGetStringFromElement(JsonObject jsonObject,String property) { 
    if (jsonObject.has(property)) { 
      return jsonObject.get(property).getAsString();
    }
    return "";
  }

  public static void stringCollectionToJsonArray(JsonObject jsonObject,
      String propertyName, Collection<String> stringSet) {
    stringCollectionToJsonArrayWithMax(jsonObject, propertyName, stringSet,
        Integer.MAX_VALUE);
  }

  public static void safeJsonArrayToStringCollection(JsonObject jsonObject,
      String propertyName, Collection<String> stringSet) {
    JsonArray array = jsonObject.getAsJsonArray(propertyName);
    if (array != null) {
      for (JsonElement element : array) {
        stringSet.add(element.getAsString());
      }
    }
  }

  public static void stringCollectionToJsonArrayWithMax(JsonObject jsonObject,
      String propertyName, Collection<String> stringSet, int maxAmount) {
    JsonArray array = new JsonArray();
    int itemCount = 0;
    for (String value : stringSet) {
      array.add(new JsonPrimitive(value));
      if (++itemCount >= maxAmount)
        break;
    }
    jsonObject.add(propertyName, array);
  }

}

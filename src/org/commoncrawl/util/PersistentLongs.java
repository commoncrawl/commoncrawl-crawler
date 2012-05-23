package org.commoncrawl.util;

import java.io.*;

/**
 *
 */
public class PersistentLongs
{
    //--------------------------------------------------------------------
    private PersistentLongs() {}


    //--------------------------------------------------------------------
    public static long[] retrieve(String fromFile)
    {
        return retrieve( new File(fromFile) );
    }
    public static long[] retrieve(File fromFile)
    {
        try
        {
            return doRetrieve(fromFile);
        }
        catch (Exception e)
        {
            throw new Error( e );
        }
    }

    private static long[] doRetrieve(File cacheFile) throws Exception
    {
        if (! cacheFile.canRead()) return null;

        long[] cached = new long[ (int)(cacheFile.length() / 8) ];

        DataInputStream cache =
                new DataInputStream(
                        new BufferedInputStream(
                                new FileInputStream(cacheFile),
                                1048576));
        for (int i = 0; i < cached.length; i++)
        {
            cached[ i ] = cache.readLong();
        }
        cache.close();

        return cached;
    }

    //--------------------------------------------------------------------
    public static void persist(
            long vals[], String fileName)
    {
        persist( vals, new File(fileName) );
    }
    public static void persist(
            long vals[], File toFile)
    {
        try
        {
            doPersist(vals, toFile);
        }
        catch (Exception e)
        {
            throw new Error( e );
        }
    }

    private static void doPersist(
            long vals[], File cacheFile) throws Exception
    {
        //noinspection ResultOfMethodCallIgnored
        cacheFile.createNewFile();

        DataOutputStream cache =
                new DataOutputStream(
                        new BufferedOutputStream(
                                new FileOutputStream(cacheFile)));
        for (long val : vals)
        {
            cache.writeLong( val );
        }
        cache.close();
    }
}
package com.digilant.mobile;

import java.sql.SQLException;
import javax.sql.rowset.CachedRowSet;

public class CachedRowSetFactory
{
    public static CachedRowSet getCachedRowSet()
        throws SQLException
    {
        try
        {
            return (CachedRowSet) Class
                .forName("com.sun.rowset.CachedRowSetImpl").newInstance();
        }
        catch (Exception e)
        {
            throw new SQLException("Could not create a CachedRowSet", e);
        }
    }
}
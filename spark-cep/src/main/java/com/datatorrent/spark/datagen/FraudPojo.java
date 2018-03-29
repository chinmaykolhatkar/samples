package com.datatorrent.spark.datagen;

import java.io.Serializable;

/**
 * POJO for a minimal fraud like schema
 */
public class FraudPojo implements Serializable
{
  public String dataSource;
  public long timestamp;
  public String email;
  public String country;
  public String cctype;
  public String currency;
  public String isfraud;
  public long amount;

  public FraudPojo()
  {
  }

  public String getDataSource()
  {
    return dataSource;
  }

  public void setDataSource(String dataSource)
  {
    this.dataSource = dataSource;
  }

  public long getTimestamp()
  {
    return timestamp;
  }

  public void setTimestamp(long timestamp)
  {
    this.timestamp = timestamp;
  }

  public String getEmail()
  {
    return email;
  }

  public void setEmail(String email)
  {
    this.email = email;
  }

  public String getCountry()
  {
    return country;
  }

  public void setCountry(String country)
  {
    this.country = country;
  }

  public String getCctype()
  {
    return cctype;
  }

  public void setCctype(String cctype)
  {
    this.cctype = cctype;
  }

  public String getCurrency()
  {
    return currency;
  }

  public void setCurrency(String currency)
  {
    this.currency = currency;
  }

  public String getIsfraud()
  {
    return isfraud;
  }

  public void setIsfraud(String isfraud)
  {
    this.isfraud = isfraud;
  }

  public long getAmount()
  {
    return amount;
  }

  public void setAmount(long amount)
  {
    this.amount = amount;
  }

  @Override
  public String toString()
  {
    return "FraudPojo{" +
      "dataSource='" + dataSource + '\'' +
      ", timestamp=" + timestamp +
      ", email='" + email + '\'' +
      ", country='" + country + '\'' +
      ", cctype='" + cctype + '\'' +
      ", currency='" + currency + '\'' +
      ", isfraud='" + isfraud + '\'' +
      ", amount=" + amount +
      '}';
  }
}

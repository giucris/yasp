package it.yasp.core.spark.model

/** BasicCredentials model
  * @param username:
  *   a [[String]] username
  * @param password:
  *   a [[String]] password
  */
final case class BasicCredentials(
    username: String,
    password: String
)

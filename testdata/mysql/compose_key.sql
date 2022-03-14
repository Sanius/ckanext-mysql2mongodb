-- MySQL dump 10.13  Distrib 8.0.22, for Linux (x86_64)
--
-- Host: localhost    Database: compose_key
-- ------------------------------------------------------
-- Server version	8.0.22

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!50503 SET NAMES utf8mb4 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `bin_data`
--

DROP TABLE IF EXISTS `bin_data`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `bin_data` (
  `id` int NOT NULL AUTO_INCREMENT,
  `bin_code` binary(16) DEFAULT NULL,
  `varbin_code` varbinary(32) DEFAULT NULL,
  `bit_code` bit(1) DEFAULT NULL,
  `bit_code_2` bit(8) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=6 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `bin_data`
--

LOCK TABLES `bin_data` WRITE;
/*!40000 ALTER TABLE `bin_data` DISABLE KEYS */;
INSERT INTO `bin_data` VALUES (1,_binary '\0\0\0\0\0\0\0\0\0\0\0\0\0\0',_binary '\0\0',_binary '',_binary ''),(2,_binary '\0\0\0\0\0\0\0\0\0\0\0\0\0\0',_binary '',_binary '\0',_binary ''),(3,_binary '\0\0\0\0\0\0\0\0\0\0\0\0\0\0',_binary '',_binary '',_binary ''),(4,_binary '\0\0\0\0\0\0\0\0\0\0\0\0\0\0',_binary '\0',_binary '',_binary ''),(5,_binary '\0\0\0\0\0\0\0\0\0\0\0\0',_binary '\0',_binary '\0',_binary '');
/*!40000 ALTER TABLE `bin_data` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `class`
--

DROP TABLE IF EXISTS `class`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `class` (
  `class_id` int NOT NULL,
  `class_name` varchar(255) NOT NULL,
  `room_block` varchar(10) NOT NULL,
  `room_floor` int NOT NULL,
  `room_number` int NOT NULL,
  PRIMARY KEY (`class_id`),
  KEY `fk_class_room` (`room_block`,`room_floor`,`room_number`),
  CONSTRAINT `fk_class_room` FOREIGN KEY (`room_block`, `room_floor`, `room_number`) REFERENCES `room` (`room_block`, `room_floor`, `room_number`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `class`
--

LOCK TABLES `class` WRITE;
/*!40000 ALTER TABLE `class` DISABLE KEYS */;
INSERT INTO `class` VALUES (1,'Data structures and algorithms','H6',4,12),(2,'Parallel computing','H6',6,2),(3,'Operating system','A3',1,4);
/*!40000 ALTER TABLE `class` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `json_test`
--

DROP TABLE IF EXISTS `json_test`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `json_test` (
  `id` int NOT NULL,
  `ascii_dict` json DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `json_test`
--

LOCK TABLES `json_test` WRITE;
/*!40000 ALTER TABLE `json_test` DISABLE KEYS */;
INSERT INTO `json_test` VALUES (1,'{}'),(2,'{\"97\": \"a\", \"98\": \"b\", \"99\": \"c\"}');
/*!40000 ALTER TABLE `json_test` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `professor`
--

DROP TABLE IF EXISTS `professor`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `professor` (
  `id` int NOT NULL,
  `name` varchar(255) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `professor`
--

LOCK TABLES `professor` WRITE;
/*!40000 ALTER TABLE `professor` DISABLE KEYS */;
/*!40000 ALTER TABLE `professor` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `room`
--

DROP TABLE IF EXISTS `room`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `room` (
  `room_block` varchar(10) NOT NULL,
  `room_floor` int NOT NULL,
  `room_number` int NOT NULL,
  PRIMARY KEY (`room_block`,`room_floor`,`room_number`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `room`
--

LOCK TABLES `room` WRITE;
/*!40000 ALTER TABLE `room` DISABLE KEYS */;
INSERT INTO `room` VALUES ('A3',1,4),('H6',4,12),('H6',6,2);
/*!40000 ALTER TABLE `room` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `table_ori`
--

DROP TABLE IF EXISTS `table_ori`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `table_ori` (
  `id1` int NOT NULL,
  `id2` int NOT NULL,
  `name` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id1`,`id2`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `table_ori`
--

LOCK TABLES `table_ori` WRITE;
/*!40000 ALTER TABLE `table_ori` DISABLE KEYS */;
INSERT INTO `table_ori` VALUES (1,1,'a'),(1,2,'b'),(2,1,'c');
/*!40000 ALTER TABLE `table_ori` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `table_ref`
--

DROP TABLE IF EXISTS `table_ref`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `table_ref` (
  `id` int NOT NULL,
  `id1` int DEFAULT NULL,
  `id2` int DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `id1` (`id1`,`id2`),
  CONSTRAINT `table_ref_ibfk_1` FOREIGN KEY (`id1`, `id2`) REFERENCES `table_ori` (`id1`, `id2`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `table_ref`
--

LOCK TABLES `table_ref` WRITE;
/*!40000 ALTER TABLE `table_ref` DISABLE KEYS */;
INSERT INTO `table_ref` VALUES (1,1,1),(3,1,2),(2,2,1),(4,2,1);
/*!40000 ALTER TABLE `table_ref` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `table_self`
--

DROP TABLE IF EXISTS `table_self`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `table_self` (
  `id` int NOT NULL,
  `ref` int DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `ref` (`ref`),
  CONSTRAINT `table_self_ibfk_1` FOREIGN KEY (`ref`) REFERENCES `table_self` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `table_self`
--

LOCK TABLES `table_self` WRITE;
/*!40000 ALTER TABLE `table_self` DISABLE KEYS */;
INSERT INTO `table_self` VALUES (3,1),(4,1),(1,2),(2,3);
/*!40000 ALTER TABLE `table_self` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `test_double`
--

DROP TABLE IF EXISTS `test_double`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `test_double` (
  `id` int NOT NULL,
  `unit` int DEFAULT NULL,
  `weight` float DEFAULT NULL,
  `heght` double DEFAULT NULL,
  `amount` decimal(10,2) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `test_double`
--

LOCK TABLES `test_double` WRITE;
/*!40000 ALTER TABLE `test_double` DISABLE KEYS */;
INSERT INTO `test_double` VALUES (1,2,5.33,125.77,999.55),(2,4,43432.1,24325.324214,5325.53),(3,3,5435.77,3243.1213,765676.23);
/*!40000 ALTER TABLE `test_double` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `test_key`
--

DROP TABLE IF EXISTS `test_key`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `test_key` (
  `id` int DEFAULT NULL,
  KEY `id` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `test_key`
--

LOCK TABLES `test_key` WRITE;
/*!40000 ALTER TABLE `test_key` DISABLE KEYS */;
/*!40000 ALTER TABLE `test_key` ENABLE KEYS */;
UNLOCK TABLES;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2020-12-17 13:57:18

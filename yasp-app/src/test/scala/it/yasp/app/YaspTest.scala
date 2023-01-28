package it.yasp.app

import it.yasp.app.err.YaspError.{ParseYmlError, ReadFileError, YaspArgsError, YaspExecutionError}
import it.yasp.testkit.TestUtils.{cleanFolder, createFile}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class YaspTest extends AnyFunSuite with BeforeAndAfterAll {
  private val workspace = "yasp-app/src/test/resources/Yasp"

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    cleanFolder(workspace)
    createFile(
      filePath = s"$workspace/input/source/user.csv",
      rows = Seq(
        "id,name,surname,age",
        "1,tester,scala,18",
        "2,coder,spark,22"
      )
    )
    createFile(
      filePath = s"$workspace/input/source/addresses.jsonl",
      rows = Seq(
        "{\"user_id\":\"1\",\"address\":\"street1\"}",
        "{\"user_id\":\"2\",\"address\":\"street2\"}"
      )
    )
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    cleanFolder(workspace)
  }

  test("main raise illegal argument exception for bad args") {
    assertThrows[YaspArgsError] {
      Yasp.main(Array("--x", s"$workspace/yasp.yaml"))
    }
  }

  test("main raise read file error for file not found") {
    assertThrows[ReadFileError] {
      Yasp.main(Array("-f", s"$workspace/not/existing/file"))
    }
  }

  test("main raise parse file error for wrong yaml content") {
    createFile(
      filePath = s"$workspace/plan/1/yasp.yaml",
      rows = Seq(
        s"""session:
           |
           |plan:
           |  sources:
           |
           |""".stripMargin
      )
    )
    assertThrows[ParseYmlError] {
      Yasp.main(Array("-f", s"$workspace/plan/1/yasp.yaml"))
    }
  }

  test("main raise execution error for wrong config on yaml") {
    createFile(
      filePath = s"$workspace/plan/2/yasp.yaml",
      rows = Seq(
        s"""session:
           |  kind: Local
           |  name: example-app
           |  conf: {}
           |plan:
           |  actions:
           |    - id: test_x
           |      dataset: test_x
           |      source:
           |        format: csv
           |        options:
           |          path: $workspace/input/yyyy/user.csv
           |          header: 'true'
           |          sep: ','
           |      cache: Memory
           |    - id: test_y
           |      dataset: test_y
           |      dest:
           |        format: json
           |        options:
           |          path: $workspace/output/
           |""".stripMargin
      )
    )
    assertThrows[YaspExecutionError] {
      Yasp.main(Array("--file", s"$workspace/plan/2/yasp.yaml"))
    }
  }

  test("main successful execute plan dryRun") {
    createFile(
      filePath = s"$workspace/plan/3/yasp.yaml",
      rows = Seq(
        s"""
           |session:
           |  kind: Local
           |  name: example-app
           |  conf:
           |    k: j
           |    x: y
           |  withHiveSupport: true
           |  withDeltaSupport: true
           |  withCheckpointDir: $workspace/checkPoint/dir
           |plan:
           |  sources:
           |    - id: users
           |      dataset: users
           |      source:
           |        format: csv
           |        options:
           |          path: $workspace/input/source/user.csv
           |          header: 'true'
           |          sep: ','
           |      cache: Memory
           |    - id: addresses
           |      dataset: addresses
           |      source:
           |        format: json
           |        options:
           |          path: $workspace/input/source/addresses.jsonl
           |  processes:
           |    - id: user_with_address
           |      dataset: user_with_address
           |      process:
           |        query: >-
           |          SELECT u.name,u.surname,a.address
           |          FROM users u JOIN addresses a ON u.id = a.user_id
           |  sinks:
           |    - id: user_with_address
           |      dataset: user_with_address
           |      dest:
           |        format: json
           |        options:
           |          path: $workspace/output/
           |""".stripMargin
      )
    )
    Yasp.main(Array("--file", s"$workspace/plan/3/yasp.yaml", "--dry-run"))
  }

  test("main successful execute plan") {
    createFile(
      filePath = s"$workspace/plan/4/yasp.yaml",
      rows = Seq(
        s"""
           |session:
           |  kind: Local
           |  name: example-app
           |  conf:
           |    k: j
           |    x: y
           |  withHiveSupport: true
           |  withDeltaSupport: true
           |  withCheckpointDir: $workspace/checkPoint/dir
           |plan:
           |  sources:
           |    - id: read_user
           |      dataset: users
           |      source:
           |        format: csv
           |        options:
           |          path: $workspace/input/source/user.csv
           |          header: 'true'
           |          sep: ','
           |      cache: Memory
           |    - id: read_addresses
           |      dataset: addresses
           |      source:
           |        format: json
           |        options:
           |          path: $workspace/input/source/addresses.jsonl
           |  processes:
           |    - id: user_join_process
           |      dataset: user_with_addresses
           |      process:
           |        query: >-
           |          SELECT u.name,u.surname,a.address
           |          FROM users u JOIN addresses a ON u.id = a.user_id
           |  sinks:
           |    - id: sink_user_to_s3
           |      dataset: user_with_addresses
           |      dest:
           |        format: json
           |        options:
           |          path: $workspace/output/
           |""".stripMargin
      )
    )
    Yasp.main(Array("--file", s"$workspace/plan/4/yasp.yaml"))
  }

  test("main successful execute plan with iceberg catalogs") {
    createFile(
      filePath = s"$workspace/plan/5/yasp.yaml",
      rows = Seq(
        s"""
           |session:
           |  kind: Local
           |  name: example-app
           |  withHiveSupport: true
           |  withIcebergSupport: true
           |  withIcebergCatalogs:
           |    - name: local
           |      path: $workspace/plan/5/local_catalog
           |  withCheckpointDir: $workspace/checkPoint/dir
           |plan:
           |  sources:
           |    - id: read_user
           |      dataset: users_x
           |      source:
           |        format: csv
           |        options:
           |          path: $workspace/input/source/user.csv
           |          header: 'true'
           |          sep: ','
           |      cache: Memory
           |    - id: read_address
           |      dataset: addresses_x
           |      source:
           |        format: json
           |        options:
           |          path: $workspace/input/source/addresses.jsonl
           |  processes:
           |    - id: user_address
           |      dataset: user_with_address_x
           |      process:
           |        query: >-
           |          SELECT u.name,u.surname,a.address
           |          FROM users_x u JOIN addresses_x a ON u.id = a.user_id
           |    - id: init_table
           |      dataset: crate_table
           |      process:
           |        query: >-
           |          CREATE TABLE local.my_db.iceberg_users_x (
           |              name string,
           |              surname string,
           |              address string
           |          ) USING iceberg;
           |    - id: exec_instert_into
           |      dataset: insert_into
           |      process:
           |        query: >-
           |          INSERT INTO local.my_db.iceberg_users_x
           |          SELECT name, surname, address FROM user_with_address_x
           |""".stripMargin
      )
    )
    Yasp.main(Array("--file", s"$workspace/plan/5/yasp.yaml"))
  }
}

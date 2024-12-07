enablePlugins(JvmPlugin, ProtocPlugin)

lazy val root = (project in file("."))
  .settings(
    name := "DistributedSorting",
    version := "0.1",
    scalaVersion := "2.12.19",

    libraryDependencies ++= Seq(
      "io.grpc" % "grpc-netty" % "1.56.0",
      "io.grpc" % "grpc-protobuf" % "1.53.0",
      "io.grpc" % "grpc-stub" % "1.53.0",
      "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5",
      "org.slf4j" % "slf4j-simple" % "2.0.9",
      "com.thesamet.scalapb" %% "scalapb-runtime" % "0.11.6",
      "com.thesamet.scalapb" %% "compilerplugin" % "0.11.6",
      "org.scalatest" % "scalatest_2.10" % "2.2.1" % "test"
    ),

    Compile / PB.targets := Seq(
      scalapb.gen() -> (Compile / sourceManaged).value / "scalapb"
    )
  )

addSbtPlugin("com.thesamet" % "sbt-protoc" % "1.0.4")

// ScalaPB 컴파일러 플러그인 추가
libraryDependencies += "com.thesamet.scalapb" %% "compilerplugin" % "0.11.6"
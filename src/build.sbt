enablePlugins(JavaPlugin, JvmPlugin, ProtocPlugin)

lazy val root = (project in file("."))
  .settings(
    name := "DistributedSorting",
    version := "0.1",
    scalaVersion := "2.13.12",
    libraryDependencies ++= Seq(
      "io.grpc" % "grpc-netty" % "1.53.0",
      "io.grpc" % "grpc-protobuf" % "1.53.0",
      "io.grpc" % "grpc-stub" % "1.53.0",
      "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5",
      "org.slf4j" % "slf4j-simple" % "2.0.9"
    ),
    Compile / PB.targets := Seq(
      scalapb.gen(flatPackage = true) -> (Compile / sourceManaged).value / "scalapb"
    )
  )

addSbtPlugin("com.thesamet" % "sbt-protoc" % "1.0.4")

// ScalaPB code generator
libraryDependencies += "com.thesamet.scalapb" %% "compilerplugin" % "0.11.6"
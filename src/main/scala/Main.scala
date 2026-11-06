import cats.effect.{IO, IOApp, Deferred}
import scala.concurrent.duration.*

object Main extends IOApp.Simple:
  def run: IO[Unit] =
    Cache.make[IO, String, Int](ttl = 10.seconds, cleanupInterval = 1.second).use{ cache =>

      val goodLoad: IO[Int] =
        IO.sleep(1.second) *> IO.println("загружаю...") *> IO.pure(470)

      val badLoad: IO[Int] =
        IO.sleep(1.second) *> IO.println("загружаю...") *> IO.raiseError[Int](new RuntimeException("упало"))

      for
        r1 <- IO.both(cache.getOrLoad("a", goodLoad).attempt, cache.getOrLoad("a", goodLoad).attempt)
        _ <- IO.println(s"проверка 1: $r1")
        r2 <- IO.both(cache.getOrLoad("b", badLoad).attempt, cache.getOrLoad("b", badLoad).attempt)
        _ <- IO.println(s"проверка 2: $r2")
        r3 <- IO.both(cache.getOrLoad("b", goodLoad).attempt, cache.getOrLoad("b", goodLoad).attempt)
        _ <- IO.println(s"проверка 3: $r3")
      yield()
    }
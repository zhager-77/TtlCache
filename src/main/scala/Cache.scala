import cats.effect.kernel.Resource
import cats.effect.{Clock, Deferred, Ref, Temporal}
import cats.syntax.all.*
import cats.effect.syntax.all.*

import scala.concurrent.duration.FiniteDuration

trait Cache[F[_], K, V]:
  def put(key: K, value: V): F[Unit]
  def get(key: K): F[Option[V]]
  def remove(key: K): F[Unit]
  def size: F[Int]
  def getOrLoad(key: K, load: F[V]): F[V]

enum Entry[F[_], V]:
  case Ready(value: V, expiresAt: FiniteDuration)
  case Loading(deferred: Deferred[F, Either[Throwable, V]])

object Cache:
  def make[F[_]: Temporal, K, V](ttl: FiniteDuration,
                                 cleanupInterval: FiniteDuration): Resource[F, Cache[F, K, V]] = {
    for
      ref <- Resource.eval(Ref.of[F, Map[K, Entry[F, V]]](Map.empty))
      _ <- (Temporal[F].sleep(cleanupInterval) >> removeExpired(ref))
        .foreverM
        .background
    yield new Cache[F, K, V]:
        def put(key: K, value: V): F[Unit] =
          for
            now <- Clock[F].monotonic
            _ <- ref.update(cache => cache.updated(key, Entry.Ready(value, now + ttl)))
          yield ()

        def get(key: K): F[Option[V]] =
          for
            now <- Clock[F].monotonic
            cache <- ref.get
          yield cache.get(key).collect{case Entry.Ready(v, exp) if now < exp => v}

        def remove(key: K): F[Unit] =
          ref.update(cache => cache - key)

        def size: F[Int] =
          for
            now <- Clock[F].monotonic
            cache <- ref.get
          yield cache.values.count{
            case Entry.Ready(_, exp) if now < exp => true
            case _ => false
          }

        def getOrLoad(key: K, load: F[V]): F[V] =
          for
            now <- Clock[F].monotonic
            d <- Deferred[F, Either[Throwable, V]]
            v <- ref.modify{ cache =>
              cache.get(key) match {
                case Some(Entry.Ready(v, exp)) if now < exp => (cache, v.pure[F])
                case Some(Entry.Loading(other)) => (cache, other.get.rethrow)
                case _ =>
                  (cache.updated(key, Entry.Loading(d)),
                    load.attempt.flatMap{
                      case Right(data) => put(key, data) *> d.complete(Right(data)) *> data.pure[F]
                      case Left(e) => ref.update(cache => cache - key) *> d.complete(Left(e)) *> e.raiseError[F, V]
                    }
                  )
              }
            }.flatten
          yield v
    }

  private def removeExpired[F[_]: Temporal, K, V](ref: Ref[F, Map[K, Entry[F, V]]]): F[Unit] =
    for
      now <- Clock[F].monotonic
      _ <- ref.update(c => c.filter{
        case (_, Entry.Ready(_, exp)) => now < exp
        case _ => true
      })
    yield ()

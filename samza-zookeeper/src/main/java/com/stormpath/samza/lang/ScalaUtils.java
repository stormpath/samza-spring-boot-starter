package com.stormpath.samza.lang;

import org.apache.samza.SamzaException;
import scala.Option;
import scala.runtime.AbstractFunction0;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction2;

import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public class ScalaUtils {

    public static <T> Optional<T> o(scala.Option<T> scalaOption) {
        if (scalaOption.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(scalaOption.get());
    }

    public static <T> T require(Option<? extends T> option, final String msg) {
        return option.getOrElse(new AbstractFunction0<T>() {
            @Override
            public T apply() {
                throw new SamzaException(msg);
            }
        });
    }

    public static <T> T require(Option<? extends T> option, Supplier<String> supplier) {
        return option.getOrElse(new AbstractFunction0<T>() {
            @Override
            public T apply() {
                String msg = supplier.get();
                throw new SamzaException(msg);
            }
        });
    }

    public static <T> AbstractFunction0<T> val(final T val) {
        return new AbstractFunction0<T>() {
            @Override
            public T apply() {
                return val;
            }
        };
    }

    public static <T> AbstractFunction0<T> fun(final Supplier<T> fun) {
        return new AbstractFunction0<T>() {
            @Override
            public T apply() {
                return fun.get();
            }
        };
    }

    public static <T, R> AbstractFunction1<T, R> fun1(final Consumer<T> fun) {
        return new AbstractFunction1<T, R>() {
            @Override
            public R apply(T val) {
                fun.accept(val);
                return null;
            }
        };
    }

    public static <T, R> AbstractFunction1<T, R> funf(final Function<T, R> fun) {
        return new AbstractFunction1<T, R>() {
            @Override
            public R apply(T v1) {
                return fun.apply(v1);
            }
        };
    }

    public static <T1, T2, R> AbstractFunction2<T1, T2, R> fun2(final BiConsumer<T1, T2> fun) {
        return new AbstractFunction2<T1, T2, R>() {
            @Override
            public R apply(T1 v1, T2 v2) {
                fun.accept(v1, v2);
                return null;
            }
        };
    }

}

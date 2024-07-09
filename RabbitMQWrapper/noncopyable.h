//  (C) Copyright Beman Dawes 1999-2003. Distributed under the Boost
//  Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//  Contributed by Dave Abrahams
//  See http://www.boost.org/libs/utility for documentation.

#ifndef AMQP_BOOST_NONCOPYABLE_HPP_INCLUDED
#define AMQP_BOOST_NONCOPYABLE_HPP_INCLUDED


namespace AMQP
{
    class noncopyable
    {
        /*!
            This class makes it easier to declare a class as non-copyable.
            If you want to make an object that can't be copied just inherit
            from this object.
        !*/

    protected:
        noncopyable() = default;
        ~noncopyable() = default;
    private:  // emphasize the following members are private
        noncopyable(const noncopyable&);
        const noncopyable& operator=(const noncopyable&);

    };
}

#endif  // AMQP_BOOST_NONCOPYABLE_HPP_INCLUDED


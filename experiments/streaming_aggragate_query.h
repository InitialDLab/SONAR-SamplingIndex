//
// Aggregate query sentinels which will be used to calculate the result for an exact query 
// when testing the accuracy of the samples for aggregates.
//
#ifndef STREAMING_AGGRAGATE_QUERY_H__
#define STREAMING_AGGRAGATE_QUERY_H__

#include <string>

#include "mongo_types.h"

class streaming_aggragate_query
{
public:
    virtual void submit(const mongo_types::entry &element)
    {
        mongo_types::sample_entry s_element(element);
        this->submit(s_element);        
    }

    virtual void submit(const mongo_types::sample_entry &element) = 0;

    virtual double get_avg() = 0;
    virtual double get_var() = 0;
    virtual int get_min() = 0;
    virtual int get_max() = 0;
    virtual long get_count() = 0;

    virtual mongo_types::box3d get_queryBox() = 0;

protected:
    bool within(mongo_types::box3d region, mongo_types::point3d location);

private:
};

class streaming_aggragate_query_time : public streaming_aggragate_query
{
public:
    streaming_aggragate_query_time(mongo_types::box3d query_region);

    virtual void submit(const mongo_types::sample_entry &element);

    virtual double get_avg();
    virtual double get_var();
    virtual int get_min();
    virtual int get_max();
    virtual long get_count();

    virtual mongo_types::box3d get_queryBox();

private:
    mongo_types::box3d m_queryRegion;

    void update_variance(int value);

    long m_count;
    long m_total;
    int m_min;
    int m_max;
    //double get_var;

    double m_Mk;
    double m_Qk;

};


#endif
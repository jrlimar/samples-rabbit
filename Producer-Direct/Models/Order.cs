using System;

namespace Producer_Direct.Models
{
    public class Order
    {
        public long Id { get; private set; }
        public DateTime CreateDate { get; }
        public DateTime LastUpdated { get; private set; }
        public long Amount { get; private set; }
        public Order(long id, long amount)
        {
            Id = id;
            Amount = amount;
            CreateDate = LastUpdated = DateTime.UtcNow;
        }

        public void UpdateOrder(long amount)
        {
            Amount = amount;
            LastUpdated = DateTime.UtcNow;
        }
    }
}

package io.kafka.transaction.store;

import java.io.Serializable;

/**
 * 事务日志索引位置
 *
 * @author tf
 * @date 2019-6-25
 *
 */
public final class JournalLocation implements Comparable<JournalLocation>, Serializable {

    public int number;
    public long offset;
    static final long serialVersionUID = -1L;

    public JournalLocation(final int number, final long offset) {
        super();
        this.number = number;
        this.offset = offset;
    }


    public int getNumber() {
        return this.number;
    }


    public void setNumber(final int number) {
        this.number = number;
    }


    public long getOffset() {
        return this.offset;
    }


    public void setOffset(final long offset) {
        this.offset = offset;
    }


    @Override
    public int compareTo(final JournalLocation o) {
        final int rt = this.number - o.number;
        if (rt != 0) {
            return rt;
        }
        else {
            if (this.offset > o.offset) {
                return 1;
            }
            else if (this.offset < o.offset) {
                return -1;
            }
            else {
                return 0;
            }
        }

    }


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + this.number;
        result = prime * result + (int) (this.offset ^ this.offset >>> 32);
        return result;
    }


    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (this.getClass() != obj.getClass()) {
            return false;
        }
        final JournalLocation other = (JournalLocation) obj;
        if (this.number != other.number) {
            return false;
        }
        if (this.offset != other.offset) {
            return false;
        }
        return true;
    }

}
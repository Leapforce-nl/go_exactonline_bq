package exactonline_bq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"

	errortools "github.com/leapforce-libraries/go_errortools"
	crm "github.com/leapforce-libraries/go_exactonline_new/crm"
	google "github.com/leapforce-libraries/go_google"
	types "github.com/leapforce-libraries/go_types"
)

type AddressBQ struct {
	ClientID             string
	ID                   string
	Account              string
	AccountIsSupplier    bool
	AccountName          string
	AddressLine1         string
	AddressLine2         string
	AddressLine3         string
	City                 string
	Contact              string
	ContactName          string
	Country              string
	CountryName          string
	Created              bigquery.NullTimestamp
	Creator              string
	CreatorFullName      string
	Division             int32
	Fax                  string
	FreeBoolField01      bool
	FreeBoolField02      bool
	FreeBoolField03      bool
	FreeBoolField04      bool
	FreeBoolField05      bool
	FreeDateField01      bigquery.NullTimestamp
	FreeDateField02      bigquery.NullTimestamp
	FreeDateField03      bigquery.NullTimestamp
	FreeDateField04      bigquery.NullTimestamp
	FreeDateField05      bigquery.NullTimestamp
	FreeNumberField01    float64
	FreeNumberField02    float64
	FreeNumberField03    float64
	FreeNumberField04    float64
	FreeNumberField05    float64
	FreeTextField01      string
	FreeTextField02      string
	FreeTextField03      string
	FreeTextField04      string
	FreeTextField05      string
	Mailbox              string
	Main                 bool
	Modified             bigquery.NullTimestamp
	Modifier             string
	ModifierFullName     string
	NicNumber            string
	Notes                string
	Phone                string
	PhoneExtension       string
	Postcode             string
	State                string
	StateDescription     string
	Type                 int16
	Warehouse            string
	WarehouseCode        string
	WarehouseDescription string
}

func getAddressBQ(c *crm.Address, clientID string) AddressBQ {
	return AddressBQ{
		clientID,
		c.ID.String(),
		c.Account.String(),
		c.AccountIsSupplier,
		c.AccountName,
		c.AddressLine1,
		c.AddressLine2,
		c.AddressLine3,
		c.City,
		c.Contact.String(),
		c.ContactName,
		c.Country,
		c.CountryName,
		google.DateToNullTimestamp(c.Created),
		c.Creator.String(),
		c.CreatorFullName,
		c.Division,
		c.Fax,
		c.FreeBoolField01,
		c.FreeBoolField02,
		c.FreeBoolField03,
		c.FreeBoolField04,
		c.FreeBoolField05,
		google.DateToNullTimestamp(c.FreeDateField01),
		google.DateToNullTimestamp(c.FreeDateField02),
		google.DateToNullTimestamp(c.FreeDateField03),
		google.DateToNullTimestamp(c.FreeDateField04),
		google.DateToNullTimestamp(c.FreeDateField05),
		c.FreeNumberField01,
		c.FreeNumberField02,
		c.FreeNumberField03,
		c.FreeNumberField04,
		c.FreeNumberField05,
		c.FreeTextField01,
		c.FreeTextField02,
		c.FreeTextField03,
		c.FreeTextField04,
		c.FreeTextField05,
		c.Mailbox,
		c.Main,
		google.DateToNullTimestamp(c.Modified),
		c.Modifier.String(),
		c.ModifierFullName,
		c.NicNumber,
		c.Notes,
		c.Phone,
		c.PhoneExtension,
		c.Postcode,
		c.State,
		c.StateDescription,
		c.Type,
		c.Warehouse.String(),
		c.WarehouseCode,
		c.WarehouseDescription,
	}
}

func (service *Service) WriteAddressesBQ(bucketHandle *storage.BucketHandle, lastModified *time.Time) ([]*storage.ObjectHandle, int, interface{}, *errortools.Error) {
	if bucketHandle == nil {
		return nil, 0, nil, nil
	}

	objectHandles := []*storage.ObjectHandle{}
	var w *storage.Writer

	getAddressesCallparams := crm.GetAddressesCallParams{
		ModifiedAfter: lastModified,
	}

	call := service.CRMService().NewGetAddressesCall(&getAddressesCallparams)

	rowCount := 0
	batchRowCount := 0
	batchSize := 10000

	for true {
		addresses, e := call.Do()
		if e != nil {
			return nil, 0, nil, e
		}

		if addresses == nil {
			break
		}

		if batchRowCount == 0 {
			guid := types.NewGUID()
			objectHandle := bucketHandle.Object((&guid).String())
			objectHandles = append(objectHandles, objectHandle)

			w = objectHandle.NewWriter(context.Background())
		}

		for _, tl := range *addresses {
			batchRowCount++

			b, err := json.Marshal(getAddressBQ(&tl, service.ClientID()))
			if err != nil {
				return nil, 0, nil, errortools.ErrorMessage(err)
			}

			// Write data
			_, err = w.Write(b)
			if err != nil {
				return nil, 0, nil, errortools.ErrorMessage(err)
			}

			// Write NewLine
			_, err = fmt.Fprintf(w, "\n")
			if err != nil {
				return nil, 0, nil, errortools.ErrorMessage(err)
			}
		}

		if batchRowCount > batchSize {
			// Close and flush data
			err := w.Close()
			if err != nil {
				return nil, 0, nil, errortools.ErrorMessage(err)
			}
			w = nil

			fmt.Printf("#Addresses for service %s flushed: %v\n", service.ClientID(), batchRowCount)

			rowCount += batchRowCount
			batchRowCount = 0
		}
	}

	if w != nil {
		// Close and flush data
		err := w.Close()
		if err != nil {
			return nil, 0, nil, errortools.ErrorMessage(err)
		}

		rowCount += batchRowCount
	}

	fmt.Printf("#Addresses for client %s: %v\n", service.ClientID(), rowCount)

	return objectHandles, rowCount, AddressBQ{}, nil
}
